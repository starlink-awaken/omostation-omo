"""ACP stdio transport — 结构化权限代理会话生命周期.

实现 ACP v1 over stdio 的客户端侧, 提供:
- session 初始化 / capability negotiation
- prompt 提交 / 输出收集
- permission request / response (权限代理)
- cancel / timeout / 进程回收 (TERM -> wait -> KILL -> wait)
- failure-injection 测试钩子

设计依据: docs/superpowers/specs/2026-08-14-codex-acp-stdio-cutover-design.md
"""
from __future__ import annotations

import enum
import json
import os
import shlex
import signal
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


class AcpState(enum.Enum):
    """ACP 会话状态机 — 这些状态不得直接映射 WorkflowVerified."""

    NOT_STARTED = "not_started"
    PROCESS_STARTED = "process_started"
    INITIALIZED = "initialized"
    SESSION_CREATED = "session_created"
    PROMPT_ACCEPTED = "prompt_accepted"
    PERMISSION_REQUESTED = "permission_requested"
    PERMISSION_DECIDED = "permission_decided"
    MODEL_OUTPUT_OBSERVED = "model_output_observed"
    TURN_COMPLETED = "turn_completed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"
    FAILED = "failed"


class PermissionDecision(enum.Enum):
    """权限决策结果."""

    ALLOW_ONCE = "allow_once"
    DENY = "deny"
    HUMAN_REQUIRED = "human_required"


@dataclass
class PermissionRequest:
    """结构化的权限请求 — 不暴露 raw prompt 或路径."""
    packet_id: str
    assignment: str
    workflow_step: str
    agent_session: str
    operation: str
    canonical_scope_digest: str
    policy_digest: str
    observed_at: str


@dataclass
class PermissionResponse:
    """权限决策回执 — 脱敏, 不含 raw prompt/transcript/token."""
    request: PermissionRequest
    decision: PermissionDecision
    reason: str
    request_id: str


@dataclass
class AcpSessionConfig:
    """ACP 会话配置."""
    command: str
    cwd: Path
    timeout_seconds: float = 300.0
    grace_period_seconds: float = 5.0
    env: dict[str, str] = field(default_factory=dict)


@dataclass
class AcpTurnResult:
    """单次 ACP turn 的结果."""
    state: AcpState
    output: str = ""
    permission_responses: list[PermissionResponse] = field(default_factory=list)
    error: str | None = None
    duration_seconds: float = 0.0


def _build_permission_request(
    packet_id: str,
    assignment: str,
    workflow_step: str,
    agent_session: str,
    operation: str,
    canonical_scope: str,
    policy_digest: str,
) -> PermissionRequest:
    """构造结构化权限请求.

    canonical_scope 必须是 digest, 不得包含绝对路径或原文.
    """
    import hashlib
    scope_digest = hashlib.sha256(
        f"{packet_id}:{canonical_scope}".encode()
    ).hexdigest()[:16]
    return PermissionRequest(
        packet_id=packet_id,
        assignment=assignment,
        workflow_step=workflow_step,
        agent_session=agent_session,
        operation=operation,
        canonical_scope_digest=scope_digest,
        policy_digest=policy_digest,
        observed_at="",
    )


def _evaluate_permission(
    request: PermissionRequest,
    *,
    allowed_write_paths: list[str],
    forbidden_write_paths: list[str],
    raw_scope: str = "",
) -> PermissionResponse:
    """本地权限代理 — 基于声明上下文自动决策.

    决策矩阵 (依据 spec §4):
      R0 只读: 自动 allow_once
      R1 窄写 (verified write surface within scope): 自动 allow_once
      R1 越界/未知: 自动拒绝
      R2+: 返回 HUMAN_REQUIRED

    raw_scope 用于内部路径比较, 不暴露到 response 中.
    canonical_scope_digest 是 raw_scope 的 hash, 用于外部回执.
    """
    request_id = f"perm-{id(request):x}"
    scope = raw_scope or request.canonical_scope_digest

    # R0: 只读操作 — 自动允许
    if request.operation in ("read", "list", "search", "grep", "cat"):
        return PermissionResponse(
            request=request,
            decision=PermissionDecision.ALLOW_ONCE,
            reason="R0 read-only: auto-allow",
            request_id=request_id,
        )

    # R1: 写操作 — 检查 scope 是否在 allowed_write_paths 内
    scope_in_allowed = any(
        scope.endswith(allowed) or allowed in scope
        for allowed in allowed_write_paths
    )
    scope_in_forbidden = any(
        scope.endswith(forbidden) or forbidden in scope
        for forbidden in forbidden_write_paths
    )

    if scope_in_forbidden:
        return PermissionResponse(
            request=request,
            decision=PermissionDecision.DENY,
            reason="R1 write to forbidden scope: denied",
            request_id=request_id,
        )

    if scope_in_allowed:
        return PermissionResponse(
            request=request,
            decision=PermissionDecision.ALLOW_ONCE,
            reason="R1 write within verified scope: auto-allow",
            request_id=request_id,
        )

    # R2+: 需要人工审批
    return PermissionResponse(
        request=request,
        decision=PermissionDecision.HUMAN_REQUIRED,
        reason="R2+ or unknown scope: human approval required",
        request_id=request_id,
    )


class AcpStdioSession:
    """ACP stdio 会话 — 管理单个 ACP agent 的完整生命周期.

    用法:
        session = AcpStdioSession(config)
        session.initialize()
        session.create_session()
        result = session.submit_turn("prompt text", ...)
        session.cancel()
        session.reap()
    """

    def __init__(self, config: AcpSessionConfig) -> None:
        self._config = config
        self._process: subprocess.Popen | None = None
        self._state = AcpState.NOT_STARTED
        self._output_buffer: list[str] = []
        self._permission_log: list[PermissionResponse] = []

    @property
    def state(self) -> AcpState:
        return self._state

    @property
    def is_alive(self) -> bool:
        return self._process is not None and self._process.poll() is None

    def initialize(self) -> None:
        """启动 ACP 进程并初始化."""
        if self._state != AcpState.NOT_STARTED:
            raise RuntimeError(f"cannot initialize from state {self._state}")

        argv = shlex.split(self._config.command)
        env = os.environ.copy()
        env.update(self._config.env)

        self._process = subprocess.Popen(
            argv,
            cwd=str(self._config.cwd),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            start_new_session=True,  # shell=False, 独立进程组
        )
        self._state = AcpState.PROCESS_STARTED

        # Send initialize request
        self._send_message({"jsonrpc": "2.0", "method": "initialize", "id": 1})
        response = self._read_message()
        if response and "error" not in response:
            self._state = AcpState.INITIALIZED

    def create_session(self, session_id: str | None = None) -> None:
        """创建 ACP 会话."""
        if self._state != AcpState.INITIALIZED:
            raise RuntimeError(f"cannot create session from state {self._state}")

        params: dict[str, Any] = {}
        if session_id:
            params["session_id"] = session_id

        self._send_message({
            "jsonrpc": "2.0",
            "method": "session/new",
            "id": 2,
            "params": params,
        })
        response = self._read_message()
        if response and "error" not in response:
            self._state = AcpState.SESSION_CREATED

    def submit_turn(
        self,
        prompt: str,
        *,
        allowed_write_paths: list[str] | None = None,
        forbidden_write_paths: list[str] | None = None,
        packet_id: str = "",
        assignment: str = "",
        workflow_step: str = "",
    ) -> AcpTurnResult:
        """提交一个 turn, 处理 permission requests, 收集输出.

        这是核心方法: 发送 prompt, 处理中间的 permission requests,
       收集模型输出, 返回完整结果.
        """
        if self._state not in (AcpState.SESSION_CREATED, AcpState.TURN_COMPLETED):
            raise RuntimeError(f"cannot submit turn from state {self._state}")

        self._state = AcpState.PROMPT_ACCEPTED
        start_time = time.monotonic()
        permission_responses: list[PermissionResponse] = []

        # Send prompt
        self._send_message({
            "jsonrpc": "2.0",
            "method": "session/prompt",
            "id": 3,
            "params": {"prompt": prompt},
        })

        # Read responses until turn-end or error
        while True:
            elapsed = time.monotonic() - start_time
            if elapsed > self._config.timeout_seconds:
                self._state = AcpState.TIMED_OUT
                self._kill_process()
                return AcpTurnResult(
                    state=AcpState.TIMED_OUT,
                    output="\n".join(self._output_buffer),
                    permission_responses=permission_responses,
                    error=f"timeout after {elapsed:.1f}s",
                    duration_seconds=elapsed,
                )

            remaining = self._config.timeout_seconds - elapsed
            if remaining <= 0:
                self._state = AcpState.TIMED_OUT
                self._kill_process()
                return AcpTurnResult(
                    state=AcpState.TIMED_OUT,
                    output="\n".join(self._output_buffer),
                    permission_responses=permission_responses,
                    error=f"timeout after {elapsed:.1f}s",
                    duration_seconds=elapsed,
                )

            try:
                response = self._read_message(timeout=remaining)
            except TimeoutError:
                # select timed out — loop back to elapsed check
                continue

            if response is None:
                self._state = AcpState.FAILED
                return AcpTurnResult(
                    state=AcpState.FAILED,
                    output="\n".join(self._output_buffer),
                    permission_responses=permission_responses,
                    error="EOF or read error",
                    duration_seconds=time.monotonic() - start_time,
                )

            method = response.get("method", "")

            if method == "session/request_permission":
                self._state = AcpState.PERMISSION_REQUESTED
                perm_params = response.get("params", {})
                perm_request = _build_permission_request(
                    packet_id=perm_params.get("packet_id", packet_id),
                    assignment=perm_params.get("assignment", assignment),
                    workflow_step=perm_params.get("workflow_step", workflow_step),
                    agent_session=perm_params.get("agent_session", ""),
                    operation=perm_params.get("operation", ""),
                    canonical_scope=perm_params.get("canonical_scope", ""),
                    policy_digest=perm_params.get("policy_digest", ""),
                )
                perm_response = _evaluate_permission(
                    perm_request,
                    allowed_write_paths=allowed_write_paths or [],
                    forbidden_write_paths=forbidden_write_paths or [],
                    raw_scope=perm_params.get("canonical_scope", ""),
                )
                permission_responses.append(perm_response)
                self._permission_log.append(perm_response)

                # Send permission decision
                self._send_message({
                    "jsonrpc": "2.0",
                    "method": "permission/respond",
                    "id": response.get("id", 4),
                    "params": {
                        "decision": perm_response.decision.value,
                        "reason": perm_response.reason,
                        "request_id": perm_response.request_id,
                    },
                })
                self._state = AcpState.PERMISSION_DECIDED

            elif method == "session/update":
                update_params = response.get("params", {})
                if update_params.get("type") == "model_output":
                    self._state = AcpState.MODEL_OUTPUT_OBSERVED
                    content = update_params.get("content", "")
                    if content:
                        self._output_buffer.append(content)

            elif "result" in response and response.get("id") == 3:
                # Turn completed
                self._state = AcpState.TURN_COMPLETED
                result_content = response.get("result", {}).get("content", "")
                if result_content:
                    self._output_buffer.append(result_content)
                break

            elif "error" in response:
                self._state = AcpState.FAILED
                return AcpTurnResult(
                    state=AcpState.FAILED,
                    output="\n".join(self._output_buffer),
                    permission_responses=permission_responses,
                    error=str(response.get("error")),
                    duration_seconds=time.monotonic() - start_time,
                )

        return AcpTurnResult(
            state=self._state,
            output="\n".join(self._output_buffer),
            permission_responses=permission_responses,
            duration_seconds=time.monotonic() - start_time,
        )

    def cancel(self) -> None:
        """取消当前 turn — 发送 cancel 信号."""
        if self._process is None:
            return
        self._send_message({
            "jsonrpc": "2.0",
            "method": "session/cancel",
            "id": 99,
        })
        self._state = AcpState.CANCELLED

    def reap(self) -> int:
        """回收进程 — TERM -> grace wait -> KILL -> wait.

        返回进程退出码. 不确认 child/session 清零时返回 -1.
        """
        if self._process is None:
            return 0

        returncode = self._process.poll()
        if returncode is not None:
            return returncode

        # Phase 1: TERM
        try:
            os.killpg(self._process.pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass

        # Phase 2: Grace wait
        try:
            self._process.wait(timeout=self._config.grace_period_seconds)
            return self._process.returncode
        except subprocess.TimeoutExpired:
            pass

        # Phase 3: KILL
        try:
            os.killpg(self._process.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass

        # Phase 4: Final wait
        try:
            self._process.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            self._state = AcpState.FAILED
            return -1

        returncode = self._process.returncode
        if returncode is None or returncode != 0:
            self._state = AcpState.FAILED
            return -1
        return returncode

    def _send_message(self, message: dict) -> None:
        """发送 JSON-RPC 消息到 ACP 进程 stdin."""
        if self._process is None or self._process.stdin is None:
            raise RuntimeError("ACP process not started or stdin closed")
        line = json.dumps(message, ensure_ascii=False) + "\n"
        self._process.stdin.write(line.encode("utf-8"))
        self._process.stdin.flush()

    def _read_message(self, timeout: float = 30.0) -> dict | None:
        """从 ACP 进程 stdout 读取一条 JSON-RPC 消息.

        使用 select 实现超时读取, 避免阻塞.
        """
        if self._process is None or self._process.stdout is None:
            return None

        import select

        ready, _, _ = select.select(
            [self._process.stdout], [], [], timeout
        )
        if not ready:
            raise TimeoutError("ACP read timed out")

        line = self._process.stdout.readline()
        if not line:
            return None

        try:
            return json.loads(line.decode("utf-8").strip())
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None

    def _kill_process(self) -> None:
        """强制杀死进程组."""
        if self._process is None:
            return
        try:
            os.killpg(self._process.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        self._process.wait(timeout=5.0)

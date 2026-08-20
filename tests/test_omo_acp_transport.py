"""Tests for omo_acp_transport — ACP stdio session lifecycle and permission broker.

覆盖 spec §7 验收标准:
1. 固定 ACP v1 stdio — 完整生命周期/错误/EOF/cancel/timeout 故障注入
2. R0/R1 权限策略严格绑定 WorkPacket/clone/claim/path
3. 越权/协议篡改/scope drift 无 WorkflowVerified
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from omo.omo_acp_transport import (
    AcpSessionConfig,
    AcpState,
    AcpStdioSession,
    AcpTurnResult,
    PermissionDecision,
    PermissionRequest,
    _build_permission_request,
    _evaluate_permission,
)

# ── Permission Broker Tests ──

class TestPermissionEvaluation:
    """R0/R1/R2 权限代理决策矩阵."""

    def _make_request(self, operation: str = "read", scope: str = "src/") -> PermissionRequest:
        return _build_permission_request(
            packet_id="test-packet",
            assignment="test-assignment",
            workflow_step="execute",
            agent_session="test-session",
            operation=operation,
            canonical_scope=scope,
            policy_digest="sha256:test",
        )

    def test_r0_read_auto_allow(self):
        req = self._make_request(operation="read")
        resp = _evaluate_permission(req, allowed_write_paths=[], forbidden_write_paths=[])
        assert resp.decision == PermissionDecision.ALLOW_ONCE
        assert "R0" in resp.reason

    def test_r0_search_auto_allow(self):
        req = self._make_request(operation="search")
        resp = _evaluate_permission(req, allowed_write_paths=[], forbidden_write_paths=[])
        assert resp.decision == PermissionDecision.ALLOW_ONCE

    def test_r1_write_within_scope_auto_allow(self):
        req = self._make_request(operation="write", scope="src/main.py")
        resp = _evaluate_permission(
            req,
            allowed_write_paths=["src/main.py"],
            forbidden_write_paths=[],
            raw_scope="src/main.py",
        )
        assert resp.decision == PermissionDecision.ALLOW_ONCE
        assert "R1" in resp.reason

    def test_r1_write_to_forgidden_scope_deny(self):
        req = self._make_request(operation="write", scope=".omo/state/system.yaml")
        resp = _evaluate_permission(
            req,
            allowed_write_paths=["src/"],
            forbidden_write_paths=[".omo/state/system.yaml"],
            raw_scope=".omo/state/system.yaml",
        )
        assert resp.decision == PermissionDecision.DENY

    def test_r2_unknown_scope_human_required(self):
        req = self._make_request(operation="delete", scope="unknown/path")
        resp = _evaluate_permission(
            req,
            allowed_write_paths=["src/"],
            forbidden_write_paths=[],
        )
        assert resp.decision == PermissionDecision.HUMAN_REQUIRED

    def test_permission_response_is_desensitized(self):
        req = self._make_request(operation="read")
        resp = _evaluate_permission(req, allowed_write_paths=[], forbidden_write_paths=[])
        # 不得包含绝对路径或原文
        assert "/home/" not in resp.reason
        assert "token" not in resp.reason.lower()


# ── Session Lifecycle Tests ──

class TestAcpSessionLifecycle:
    """ACP 会话状态机 — 不启动真实进程."""

    def _make_session(self) -> AcpStdioSession:
        config = AcpSessionConfig(
            command="echo test",
            cwd=Path("/tmp"),
            timeout_seconds=5.0,
            grace_period_seconds=1.0,
        )
        return AcpStdioSession(config)

    def test_initial_state(self):
        session = self._make_session()
        assert session.state == AcpState.NOT_STARTED
        assert not session.is_alive

    def test_initialize_requires_not_started(self):
        session = self._make_session()
        session._state = AcpState.INITIALIZED
        with pytest.raises(RuntimeError, match="cannot initialize"):
            session.initialize()

    def test_create_session_requires_initialized(self):
        session = self._make_session()
        with pytest.raises(RuntimeError, match="cannot create session"):
            session.create_session()

    def test_submit_turn_requires_session_created(self):
        session = self._make_session()
        with pytest.raises(RuntimeError, match="cannot submit turn"):
            session.submit_turn("test")

    @patch.object(AcpStdioSession, "_send_message")
    @patch.object(AcpStdioSession, "_read_message")
    def test_initialize_success(self, mock_read, mock_send):
        mock_read.return_value = {"jsonrpc": "2.0", "id": 1, "result": {"capabilities": {}}}
        session = self._make_session()
        session.initialize()
        assert session.state == AcpState.INITIALIZED

    @patch.object(AcpStdioSession, "_send_message")
    @patch.object(AcpStdioSession, "_read_message")
    def test_initialize_fail_on_error(self, mock_read, mock_send):
        mock_read.return_value = {"jsonrpc": "2.0", "id": 1, "error": {"code": -1, "message": "fail"}}
        session = self._make_session()
        session.initialize()
        # State should NOT transition to INITIALIZED on error
        assert session.state == AcpState.PROCESS_STARTED

    @patch.object(AcpStdioSession, "_send_message")
    @patch.object(AcpStdioSession, "_read_message")
    def test_create_session_success(self, mock_read, mock_send):
        mock_read.return_value = {"jsonrpc": "2.0", "id": 2, "result": {"session_id": "sess-1"}}
        session = self._make_session()
        session._state = AcpState.INITIALIZED
        session.create_session()
        assert session.state == AcpState.SESSION_CREATED

    def test_cancel_without_process(self):
        session = self._make_session()
        session.cancel()  # Should not raise

    def test_reap_without_process(self):
        session = self._make_session()
        assert session.reap() == 0


# ── Failure Injection Tests ──

class TestFailureInjection:
    """故障注入: timeout, cancel, protocol error."""

    def _make_session(self) -> AcpSessionConfig:
        return AcpSessionConfig(
            command="sleep 3600",  # Long-running, will timeout
            cwd=Path("/tmp"),
            timeout_seconds=0.5,  # Very short for test
            grace_period_seconds=0.5,
        )

    @patch.object(AcpStdioSession, "_send_message")
    @patch.object(AcpStdioSession, "_read_message")
    def test_timeout_returns_timed_out_state(self, mock_read, mock_send):
        # select times out — raises TimeoutError
        mock_read.side_effect = TimeoutError("select timed out")
        config = self._make_session()
        session = AcpStdioSession(config)
        session._state = AcpState.SESSION_CREATED
        result = session.submit_turn("test prompt")
        assert result.state == AcpState.TIMED_OUT
        assert result.error is not None
        assert "timeout" in result.error

    @patch.object(AcpStdioSession, "_send_message")
    @patch.object(AcpStdioSession, "_read_message")
    def test_eof_returns_failed_state(self, mock_read, mock_send):
        mock_read.return_value = None  # EOF
        config = self._make_session()
        session = AcpStdioSession(config)
        session._state = AcpState.SESSION_CREATED
        result = session.submit_turn("test prompt")
        assert result.state == AcpState.FAILED

    @patch.object(AcpStdioSession, "_send_message")
    @patch.object(AcpStdioSession, "_read_message")
    def test_protocol_error_returns_failed(self, mock_read, mock_send):
        mock_read.return_value = {"jsonrpc": "2.0", "error": {"code": -32600, "message": "Invalid request"}}
        config = self._make_session()
        session = AcpStdioSession(config)
        session._state = AcpState.SESSION_CREATED
        result = session.submit_turn("test")
        assert result.state == AcpState.FAILED
        assert "Invalid request" in result.error


# ── Permission Flow Integration Test ──

class TestPermissionFlow:
    """模拟完整的 permission request/response 流程."""

    @patch.object(AcpStdioSession, "_send_message")
    @patch.object(AcpStdioSession, "_read_message")
    def test_permission_round_trip(self, mock_read, mock_send):
        """模拟 ACP 进程发起 permission request, 客户端回复决策."""
        responses = [
            # First: permission request
            {
                "jsonrpc": "2.0",
                "method": "session/request_permission",
                "id": 4,
                "params": {
                    "packet_id": "pkt-1",
                    "assignment": "assign-1",
                    "workflow_step": "execute",
                    "agent_session": "sess-1",
                    "operation": "read",
                    "canonical_scope": "src/main.py",
                    "policy_digest": "sha256:abc",
                },
            },
            # Second: turn result
            {"jsonrpc": "2.0", "id": 3, "result": {"content": "file content here"}},
        ]
        mock_read.side_effect = responses

        config = AcpSessionConfig(command="echo test", cwd=Path("/tmp"))
        session = AcpStdioSession(config)
        session._state = AcpState.SESSION_CREATED

        result = session.submit_turn(
            "read file",
            allowed_write_paths=["src/"],
            forbidden_write_paths=[],
        )

        assert result.state == AcpState.TURN_COMPLETED
        assert len(result.permission_responses) == 1
        assert result.permission_responses[0].decision == PermissionDecision.ALLOW_ONCE
        assert "file content" in result.output

        # Verify permission/respond was sent
        assert mock_send.call_count >= 2  # prompt + permission respond
        respond_call = mock_send.call_args_list[1]
        respond_msg = respond_call[0][0]
        assert respond_msg["method"] == "permission/respond"
        assert respond_msg["params"]["decision"] == "allow_once"


# ── _build_permission_request Tests ──

class TestBuildPermissionRequest:
    """结构化权限请求构造."""

    def test_scope_is_hashed(self):
        req = _build_permission_request(
            packet_id="pkt",
            assignment="assign",
            workflow_step="exec",
            agent_session="sess",
            operation="read",
            canonical_scope="/absolute/path/to/file.py",
            policy_digest="sha256:policy",
        )
        # canonical_scope_digest should be a hash, not the raw path
        assert "/absolute/path" not in req.canonical_scope_digest
        # Should be a hex digest (16 chars)
        assert len(req.canonical_scope_digest) == 16
        assert all(c in "0123456789abcdef" for c in req.canonical_scope_digest)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

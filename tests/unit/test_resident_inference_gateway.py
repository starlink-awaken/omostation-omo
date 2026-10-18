#!/usr/bin/env python3
"""tests/unit/test_resident_inference_gateway.py — 单元测试跨机推断网关 (Phase 9)."""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from omo.resident.inference_gateway import SwarmInferenceGateway
from omo.resident.swarm_mesh import SwarmComputeMesh, SwarmNodeSpec


def test_inference_gateway_openai_success():
    with tempfile.TemporaryDirectory() as tmp_dir:
        ws = Path(tmp_dir)
        gateway = SwarmInferenceGateway(workspace_root=ws)

        mock_resp = {"choices": [{"message": {"content": "Hello from OpenAI compatible endpoint"}}]}
        with patch.object(
            gateway, "_call_openai_compatible", return_value=(True, "Hello from OpenAI compatible endpoint", mock_resp)
        ):
            res = gateway.chat_completion(
                messages=[{"role": "user", "content": "hello"}],
                tier="tier1_inspection",
            )
            assert res.content == "Hello from OpenAI compatible endpoint"
            assert res.fallback_used is False
            assert res.latency_ms >= 0


def test_inference_gateway_ollama_fallback_protocol():
    with tempfile.TemporaryDirectory() as tmp_dir:
        ws = Path(tmp_dir)
        gateway = SwarmInferenceGateway(workspace_root=ws)

        mock_resp = {"message": {"content": "Hello from Ollama native endpoint"}}
        # OpenAI 协议返回失败，Ollama 原生返回成功
        with (
            patch.object(gateway, "_call_openai_compatible", return_value=(False, "", {})),
            patch.object(
                gateway, "_call_ollama_native", return_value=(True, "Hello from Ollama native endpoint", mock_resp)
            ),
        ):
            res = gateway.chat_completion(
                messages=[{"role": "user", "content": "hello"}],
                tier="tier1_inspection",
            )
            assert res.content == "Hello from Ollama native endpoint"


def test_inference_gateway_failover_to_local_mac():
    with tempfile.TemporaryDirectory() as tmp_dir:
        ws = Path(tmp_dir)
        gateway = SwarmInferenceGateway(workspace_root=ws)

        # 模拟远程所有端口完全超时/不可达，最终降级回本地旗舰机仿真兜底
        with (
            patch.object(gateway, "_call_openai_compatible", return_value=(False, "", {})),
            patch.object(gateway, "_call_ollama_native", return_value=(False, "", {})),
        ):
            res = gateway.chat_completion(
                messages=[{"role": "user", "content": "ping"}],
                tier="tier1_inspection",
            )
            assert res.target_node == "local-mac"
            assert res.fallback_used is True
            assert "离线兜底输出" in res.content

            # 验证指标持久化
            metrics = gateway.get_metrics_summary()
            assert metrics["total_requests"] == 1
            assert metrics["fallback_count"] == 1
            assert "local-mac" in metrics["nodes"]

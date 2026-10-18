"""Unit tests for Phase 11 Multimodal Perception and Voice Bridge."""

import base64
import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from omo.resident.chat_engine import ChatMessage, ResidentChatEngine, persist_media_image
from omo.resident.inference_gateway import InferenceResult, SwarmInferenceGateway
from omo.resident.interact import ask, execute_react_tool
from omo.resident.screen_probe import capture_screen, extract_image_text
from omo.resident.voice_bridge import clean_text_for_speech, speak_text


def test_voice_bridge_cleaning():
    raw_text = """
    这是核心结论。
    ```python
    def dangerous_code():
        os.system("rm -rf /")
    ```
    请参考文档 [链接名称](https://example.com/docs)。
    | 序号 | 字段 |
    | --- | --- |
    | 1   | val  |
    建议执行 `git status` 确认。
    """
    cleaned = clean_text_for_speech(raw_text)
    assert "dangerous_code" not in cleaned
    assert "os.system" not in cleaned
    assert "https://example.com/docs" not in cleaned
    assert "链接名称" in cleaned
    assert "这是核心结论" in cleaned


def test_voice_bridge_speak(tmp_path: Path):
    out_file = tmp_path / "test_voice.aiff"
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)
        res = speak_text("你好，这是语音测试", voice="Tingting", play=False, output_path=out_file)
        assert res["success"] is True
        assert res["engine"] in ("say", "aetherforge")
        assert "语音测试" in res["cleaned_text"]


def test_screen_probe_capture_and_ocr(tmp_path: Path):
    # 构造假图片
    dummy_img = tmp_path / "dummy.png"
    dummy_img.write_bytes(b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR...")
    ocr_res = extract_image_text(dummy_img)
    assert isinstance(ocr_res, str)
    assert len(ocr_res) > 0


def test_chat_engine_multimodal_ocr_fallback(tmp_path: Path):
    dummy_media = tmp_path / "screen_err.png"
    dummy_media.write_bytes(b"\x89PNG fake image content")

    mock_gateway = MagicMock(spec=SwarmInferenceGateway)
    mock_gateway.chat_completion.return_value = InferenceResult(
        content="已根据附带截图研判完成，发现配置异常。",
        target_node="local-mac",
        target_host="127.0.0.1",
        target_port=1234,
        tier="tier2_dialog",
        latency_ms=120.0,
        fallback_used=False,
        model_used="qwen2.5:32b",
    )

    engine = ResidentChatEngine(workspace_root=tmp_path, gateway=mock_gateway)
    with patch("omo.resident.screen_probe.extract_image_text", return_value="Error: Connection refused at port 8000"):
        msg = engine.chat(
            prompt="看看这个错误怎么解",
            session_id="test_media_sess",
            images=[str(dummy_media)],
        )

        assert msg.role == "assistant"
        assert "研判完成" in msg.content
        assert msg.metadata.get("image_ocr_fallback") is True
        assert len(msg.metadata.get("images", [])) == 1

        # 检查传入大模型的 user prompt 是否拼入了 OCR 提取文本
        sent_messages = mock_gateway.chat_completion.call_args[1]["messages"]
        user_message_content = sent_messages[-1]["content"]
        assert "Connection refused at port 8000" in user_message_content


def test_react_probe_screen_state():
    with patch("omo.resident.screen_probe.capture_screen") as mock_cap:
        fake_path = Path("/tmp/fake_screen.png")
        fake_path.touch()
        mock_cap.return_value = fake_path

        res_raw = execute_react_tool("probe_screen_state", "fullscreen=False")
        data = json.loads(res_raw)
        assert data["status"] == "captured"
        assert "fake_screen.png" in data["image_path"]


def test_interact_ask_multimodal():
    with patch("omo.resident.chat_engine.ResidentChatEngine.chat") as mock_chat:
        mock_chat.return_value = ChatMessage(
            role="assistant",
            content="语音与图像已处理",
            metadata={"latency_ms": 10.0},
        )
        reply = ask("测试多模态接口", images=["/tmp/img.png"], voice=False)
        assert reply == "语音与图像已处理"
        assert mock_chat.call_args[1]["images"] == ["/tmp/img.png"]

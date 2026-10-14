"""BET-Y2Q4-T10-239 ISC-2 / ISC-3 / ISC-6 —— `src/**` 仓根 host 字面量的扫描判据。

一把尺不够。两条行尺（ISC-2 字面量、ISC-3 假缝）都有同一个盲区：**它们按行匹配，
所以跨行拼的路径和合并进一个字符串面量的路径都看不见**。实测两个形状：

* `omo_observability.py:37` —— `Path.home()` 与各个路径段分行写；
* `omo_self_healing.py:43` —— `Path.home() / "Workspace/projects/omo"`，正则要求
  `Workspace` 后面紧跟闭引号，于是错过。

只用行尺报「改后 0」是一个**假零**（本 BET 真的先得出过这个假零）。所以第三条尺走
AST，对排版免疫；三条尺同一次扫描同时产出「改前 N」和「改后 0」两个数（AGENTS.md §7⑤
「『改后为 0』必须与『改前为 N』由同一次扫描产出」）。「改前」一侧由
`omo_src_root_literal_baseline.json` 提供 —— 那是 d8ca1a2 上 18 个文件 32 个站点的
最内层语句快照，语料本身在断言里重算出 32/21/34，所以基线不是记忆而是可复算的。
不落 `git show d8ca1a2:` 是因为 PASW 子树检出可能只有 1 个 commit（AGENTS.md §6），
那样基线读取在 CI 与子树里都不可移植。
"""

from __future__ import annotations

import ast
import json
import re
import textwrap
from pathlib import Path

import pytest

from omo.omo_paths import OMO_SRC_PARENT

BASELINE_CORPUS = Path(__file__).resolve().parent / "omo_src_root_literal_baseline.json"

# --- ISC-2: 仓根 host 字面量（行尺，spec §2 用同一把尺：[[:space:]] 不用 \s） ---
ISC2_LINE = re.compile(r'Path\.home\(\)[ \t]*/[ \t]*"Workspace"')
# --- ISC-3: 假缝 = 读 WORKSPACE_ROOT 这个 env 名 **且** 默认值是 host 字面量 ---
ENV_READ_LINE = re.compile(r'(?:\.get\(|getenv\()\s*["\']WORKSPACE_ROOT["\']')
HOST_LITERAL_LINE = re.compile(r'Path\.home\(\)|["\']/Users/')

HOME_ANCHOR_ATTRS = frozenset({"home", "expanduser"})

# d8ca1a2 基线的三个钉死读数（ISC-2 的 32 是 spec §1 的表，ISC-3 的 21 同理，
# AST 尺的 34 = 32 + 上面那两种行尺看不见的写法）。
PINNED_BASELINE = {"isc2": 32, "isc3": 21, "ast": 34}


def is_host_literal(value: str) -> bool:
    """仓根字面量的两种写法：`"Workspace/..."` 相对段，或含 Workspace 的绝对 host 路径。"""
    if value.split("/")[0] == "Workspace":
        return True
    return value.startswith(("/Users/", "/home/")) and "Workspace" in value


def _home_anchor_ids(tree: ast.AST) -> set[int]:
    return {
        id(node)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in HOME_ANCHOR_ATTRS
    }


def _has_anchor(node: ast.AST, anchors: set[int]) -> bool:
    return any(id(sub) in anchors for sub in ast.walk(node))


def ast_hits(text: str) -> list[str]:
    """AST 尺：对换行/缩进免疫。命中项返回源码片段，便于报错时直接指认。"""
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return []
    anchors = _home_anchor_ids(tree)
    out: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
            for left, right in ((node.left, node.right), (node.right, node.left)):
                if (
                    isinstance(right, ast.Constant)
                    and isinstance(right.value, str)
                    and is_host_literal(right.value)
                    and _has_anchor(left, anchors)
                ):
                    out.append((ast.get_source_segment(text, node) or "").replace("\n", " "))
                    break
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in {"get", "getenv"}
            and len(node.args) >= 2
            and isinstance(node.args[1], ast.Constant)
            and isinstance(node.args[1].value, str)
        ):
            value: str = node.args[1].value
            if is_host_literal(value) or _has_anchor(node.args[1], anchors):
                out.append((ast.get_source_segment(text, node) or "").replace("\n", " "))
    return out


def scan_tree(root: Path) -> dict[str, list[str]]:
    """一次扫描产出三条尺的读数（root 通常是某个 `src/` 目录）。"""
    isc2: list[str] = []
    isc3: list[str] = []
    tree_hits: list[str] = []
    for path in sorted(root.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        rel = path.relative_to(root).as_posix()
        text = path.read_text(encoding="utf-8")
        for number, line in enumerate(text.splitlines(), 1):
            if ISC2_LINE.search(line):
                isc2.append(f"{rel}:{number}")
            if ENV_READ_LINE.search(line) and HOST_LITERAL_LINE.search(line):
                isc3.append(f"{rel}:{number}")
        for seg in ast_hits(text):
            tree_hits.append(f"{rel}: {seg[:120]}")
    return {"isc2": isc2, "isc3": isc3, "ast": tree_hits}


def corpus_source(snippets: list[dict]) -> str:
    """把基线语料的一个文件重建成可解析模块：每条语句包进自己的 `_shim_N`。"""
    body = []
    for index, item in enumerate(snippets):
        indented = "\n".join("    " + line if line.strip() else line for line in item["text"].splitlines())
        body.append(f"def _shim_{index}():\n{indented}\n")
    return "\n".join(body)


def build_corpus_tree(root: Path) -> Path:
    corpus = json.loads(BASELINE_CORPUS.read_text(encoding="utf-8"))
    src = root / "src"
    for rel, snippets in corpus.items():
        target = src / Path(rel).relative_to("src")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(corpus_source(snippets), encoding="utf-8")
    return src


def test_baseline_corpus_exists_and_is_host_neutral() -> None:
    """语料是「改前」的一侧，且不得携带任何 host 身份（AGENTS.md §7 隐私面）。"""
    assert BASELINE_CORPUS.is_file(), f"缺基线语料: {BASELINE_CORPUS}"
    text = BASELINE_CORPUS.read_text(encoding="utf-8")
    corpus = json.loads(text)
    assert len(corpus) == 18, f"基线语料应有 18 个文件，实际 {len(corpus)}"
    assert sum(len(v) for v in corpus.values()) == 32, "基线语料应有 32 个站点"
    assert "/Users" not in text, "基线语料不得含 host 绝对路径"


def test_baseline_corpus_reproduces_pinned_before_counts(tmp_path: Path) -> None:
    """ISC-2/ISC-3/AST 三条尺在语料上复算出 32 / 21 / 34 —— 「改前 N」不是引用而是测量。"""
    readings = scan_tree(build_corpus_tree(tmp_path))
    assert {key: len(value) for key, value in readings.items()} == PINNED_BASELINE, json.dumps(
        {key: value for key, value in readings.items() if len(value) != PINNED_BASELINE[key]},
        indent=2,
    )


def test_live_src_reads_zero_on_all_three_rulers() -> None:
    """ISC-2/ISC-3 的「改后 0」与「改前 N」由同一次扫描产出；无豁免清单（spec §6.1）。"""
    src = OMO_SRC_PARENT / "src"
    assert src.is_dir(), f"扫描锚指向不存在的路径: {src}"
    readings = scan_tree(src)
    assert readings == {"isc2": [], "isc3": [], "ast": []}, json.dumps(readings, indent=2)


def test_live_scan_is_not_a_no_op() -> None:
    """空绿反证：扫描确实吃到整棵 src，而不是锚错目录后恒 0。"""
    src = OMO_SRC_PARENT / "src"
    py_files = {p for p in src.rglob("*.py") if "__pycache__" not in p.parts}
    assert len(py_files) > 100, f"src 只数到 {len(py_files)} 个 .py —— 锚错了"
    assert (src / "omo" / "omo_trail.py") in py_files
    assert (src / "omo" / "sovereignty" / "enforcement.py") in py_files
    # 同一棵树上，被禁止的形状一定不命中，而被要求的形状（缝调用）一定命中若干次，
    # 否则「0」可能来自尺失效。
    seam_pattern = re.compile(
        r"^(?:from omo\.omo_paths import .*|.*\b(?:code_root|state_root|event_ledger_path)\(\))", re.M
    )
    seam_reads = sum(1 for path in py_files if seam_pattern.search(path.read_text(encoding="utf-8")))
    assert seam_reads > 10, f"只有 {seam_reads} 个文件引用根 resolver，缝没接上"


def test_isc2_ruler_names_an_injected_literal(tmp_path: Path) -> None:
    """ISC-6 no-op 反证①：注入被禁止的写法，行尺必须点名它。"""
    target = tmp_path / "omo" / "injected_isc2.py"
    target.parent.mkdir(parents=True)
    target.write_text(
        textwrap.dedent(
            """
            from pathlib import Path

            ROOT = Path.home() / "Workspace" / ".omo"
            """
        ),
        encoding="utf-8",
    )
    readings = scan_tree(tmp_path)
    assert len(readings["isc2"]) == 1 and readings["isc2"][0].startswith("omo/injected_isc2.py:"), readings
    assert len(readings["ast"]) == 1, readings
    assert len(readings["isc3"]) == 0, "没读 WORKSPACE_ROOT 的行不该被假缝尺命中"


def test_isc3_ruler_names_an_injected_fake_seam(tmp_path: Path) -> None:
    """ISC-6 no-op 反证②：假缝（读同名 env + host 字面量兜底）必须按「组合」命中。"""
    target = tmp_path / "omo" / "injected_isc3.py"
    target.parent.mkdir(parents=True)
    target.write_text(
        textwrap.dedent(
            """
            import os
            from pathlib import Path

            FAKE = os.environ.get("WORKSPACE_ROOT", str(Path.home() / "Workspace"))
            """
        ),
        encoding="utf-8",
    )
    readings = scan_tree(tmp_path)
    assert len(readings["isc3"]) == 1 and readings["isc3"][0].startswith("omo/injected_isc3.py:"), readings
    assert len(readings["ast"]) == 1, readings
    # 这一形状同时是「字面量」和「假缝」，两把尺都该命中 —— 交叉命中是预期，不是重复计数缺陷。
    assert len(readings["isc2"]) == 1, readings


def test_isc3_does_not_fire_on_a_legitimate_seam(tmp_path: Path) -> None:
    """ISC-3 判据落在「读该 env 且兜底是 host 字面量」这一组合，不是落在名字上。

    `mcp_server.py:21` 那种读同名 env、但兜底已是 `__file__` 反推的合法形态不计入 ——
    按名字计数会砸掉对外部 MCP host 已发布的契约名（spec §1）。
    """
    target = tmp_path / "omo" / "legit_seam.py"
    target.parent.mkdir(parents=True)
    target.write_text(
        textwrap.dedent(
            """
            import os
            from pathlib import Path

            HERE = Path(__file__).resolve().parents[2]
            ROOT = Path(os.environ.get("WORKSPACE_ROOT", str(HERE)))
            CODE = Path(os.environ.get("OMOSTATION_STATE_ROOT", str(HERE))) / ".omo"
            """
        ),
        encoding="utf-8",
    )
    readings = scan_tree(tmp_path)
    assert readings == {"isc2": [], "isc3": [], "ast": []}, readings


def test_ast_ruler_names_the_two_spellings_the_line_rulers_miss(tmp_path: Path) -> None:
    """行尺的盲区必须由 AST 尺补上 —— 这条用例是「一把尺凑出 0」的直接反证。

    两种写法各一棵树：行尺都读 0（即「假零」），AST 尺各读 1。
    """
    multiline = tmp_path / "multiline" / "omo" / "observability_like.py"
    multiline.parent.mkdir(parents=True)
    multiline.write_text(
        textwrap.dedent(
            """
            from pathlib import Path

            KNOWLEDGE_DIR = (
                Path.home()
                / "Workspace"
                / ".omo"
                / "_knowledge"
            )
            """
        ),
        encoding="utf-8",
    )
    combined = tmp_path / "combined" / "omo" / "self_healing_like.py"
    combined.parent.mkdir(parents=True)
    combined.write_text(
        textwrap.dedent(
            """
            from pathlib import Path

            PROJECT = Path.home() / "Workspace/projects/omo"
            """
        ),
        encoding="utf-8",
    )

    for root in (tmp_path / "multiline", tmp_path / "combined"):
        readings = scan_tree(root)
        assert readings["isc2"] == [], f"行尺本应看不见这种排版: {readings['isc2']}"
        assert len(readings["ast"]) == 1, f"AST 尺必须点名它: {root} -> {readings['ast']}"


def test_ast_ruler_is_not_over_wide() -> None:
    """过宽的尺靠「把读根全塞进豁免清单」也能通过 —— 所以这里反证它不打合法形态。"""
    text = textwrap.dedent(
        """
        from pathlib import Path

        from omo.omo_paths import code_root, state_root

        A = code_root() / "projects" / "runtime" / "scripts" / "notify-alerts.sh"
        B = state_root() / ".omo" / "_knowledge" / "omo-trail.jsonl"
        C = Path.home() / ".cache" / "omo"
        D = Path.home().expanduser() / "Documents"
        """
    )
    assert ast_hits(text) == [], ast_hits(text)


@pytest.mark.parametrize(
    ("expression", "expected_hits"),
    [
        ('Path.home() / "Workspace"', 1),
        ('Path.home().expanduser() / "Workspace/x"', 1),
        ('Path.home() / "Documents"', 0),
        ('Path("/Users/xiamingxing/Workspace")', 0),
        ('os.environ.get("WORKSPACE_ROOT", str(Path.home() / "Workspace"))', 1),
        ('os.getenv("OMO_DIR", str(code_root()))', 0),
    ],
)
def test_ast_ruler_hit_counts_per_shape(expression: str, expected_hits: int) -> None:
    """逐形状标定尺的命中面（含两条不该命中的邻接形状）。"""
    source = f"import os\nfrom pathlib import Path\n\nfrom omo.omo_paths import code_root\n\nX = {expression}\n"
    assert len(ast_hits(source)) == expected_hits, ast_hits(source)

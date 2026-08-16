import os
from pathlib import Path


def get_c2g_data_dir(start: Path | None = None) -> Path:
    """local adapter 数据目录 (bets.json/tasks.json/pitches/) 的稳定锚点.

    修复 P16 漂移 (2026-07-02 审计): 原先锚定 Path.cwd(), c2g 在不同 cwd 运行会把
    bets.json 散落到 workspace 根 / .omo/ / .c2g_data/ 三处并各自分叉.

    解析顺序:
      1. 环境变量 C2G_DATA_DIR (显式覆盖)
      2. 从 start (默认 cwd) 向上找最近的仓库根 (.git 或 .omo 所在目录, 跳过 ~)
         → <repo_root>/.c2g_data  (在 c2g 子仓库内运行 → c2g/.c2g_data, 保持测试语义;
            在 workspace 任意子目录运行 → workspace/.c2g_data)
      3. 兜底: <start>/.c2g_data
    """
    env = os.environ.get("C2G_DATA_DIR")
    if env:
        d = Path(env).expanduser()
        d.mkdir(parents=True, exist_ok=True)
        return d
    origin = (start or Path.cwd()).resolve()
    home = Path.home()
    cur = origin
    while cur != cur.parent:
        if cur == home:
            break
        if (cur / ".git").exists() or (cur / ".omo").is_dir():
            d = cur / ".c2g_data"
            d.mkdir(parents=True, exist_ok=True)
            return d
        cur = cur.parent
    d = origin / ".c2g_data"
    d.mkdir(parents=True, exist_ok=True)
    return d


def get_omo_dir(base_dir: Path) -> Path:
    """向上查找 .omo/, 返回最外层 (workspace 边界内) 的.

    排除 home 目录 (~) 的 .omo/ (系统级 omostation 安装, 非项目 workspace 候选).
    否则 workspace 在 ~/ 下时, found[-1] 会误返 ~/.omo/ (test_bet_id_reuse 回归:
    bet 找不到 goals/current.yaml). test_smoke 场景: workspace 内嵌套
    projects/.omo/ 时, 外层 workspace_omo (found[-1]) 优先于内层 inner_omo (found[0]).
    """
    home = Path.home()
    current = base_dir.resolve()
    found: list[Path] = []
    while current != current.parent:
        if current == home:
            current = current.parent
            continue
        omo = current / ".omo"
        if omo.is_dir():
            found.append(omo)
        current = current.parent
    if found:
        return found[-1]
    return base_dir / ".omo"


def strip_frontmatter(text: str) -> str:
    """Strip YAML frontmatter (``---`` ... ``---``) 取 body (single document).

    .omo 数据文件被 P45 doc-lifecycle 治理统一加了 frontmatter (status/lifecycle/
    owner/last-reviewed), 使其变成 multi-document YAML. ``yaml.safe_load`` 只支持
    single document, 遇到第二个 ``---`` 会抛 ComposerError. 本函数返回 frontmatter
    之后的 body, 让 parser 正常解析.
    """
    if not text.lstrip().startswith("---"):
        return text
    lines = text.split("\n")
    started = False
    for i, line in enumerate(lines):
        if line.strip() == "---":
            if not started:
                started = True
            else:
                return "\n".join(lines[i + 1 :])
    return text

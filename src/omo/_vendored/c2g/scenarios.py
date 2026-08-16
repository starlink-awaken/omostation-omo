"""c2g.scenarios — 场景注册表 (BET-6 原语多场景).

c2g 5 原语 (brainstorm/draft/bet/radar/gc) 默认通用, BET-6 让 brainstorm/draft
感知场景上下文, 生成场景化 Pitch (公文/知识/学习/家庭/健康/财务).

设计 (KISS + DRY):
  - 注册表驱动: 6 场景定义在一个 dict, brainstorm/draft 按 --scenario 读
  - 向后兼容: 不带 --scenario → 通用模板 (现有行为不变)
  - 场景化 = 场景专属小节 + upstream 提示 + YAGNI 禁区, 非场景特定原语 (不重复造轮)

与 cockpit 门户 (C 方案) 呼应: cockpit finance/gongwen 做门户引导,
c2g brainstorm --scenario finance 做引擎物化 (场景化 Pitch → Bet → Task).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Scenario:
    """场景定义 — Pitch 模板片段 + 元数据."""

    key: str  # gongwen / vault / ...
    label: str  # 公文 / 知识 / ...
    upstream_hint: str  # 北极星提示 (帮用户填 Upstream)
    nogos: list[str]  # YAGNI 默认禁区
    sections: list[tuple[str, str]]  # 场景专属小节 (title, placeholder)


SCENARIOS: dict[str, Scenario] = {
    "gongwen": Scenario(
        key="gongwen",
        label="公文",
        upstream_hint="e.g. 卫健委/国转中心公文规范落实 / 国资委13号令合规",
        nogos=["不实现审批流引擎 (留 @公文 域)", "不硬编码文种模板 (注册表驱动)"],
        sections=[
            ("文种", "通知/报告/请示/纪要/函 (选一)"),
            ("规范要素", "标题/主送机关/正文/落款/成文日期/印章"),
            ("接收对象", "上级/下级/不相隶属机关"),
        ],
    ),
    "vault": Scenario(
        key="vault",
        label="知识",
        upstream_hint="e.g. 织星知识图谱 / KOS 跨域搜索 / 学习进化",
        nogos=[
            "不引入新存储引擎 (用 gbrain/SharedBrain)",
            "不破坏 KOS 实体 ID 前缀规则",
        ],
        sections=[
            ("知识域", "facts/inferences/scheme/entities"),
            ("关联实体", "D-F*/INF-*/ORG-*/PRJ-* 引用"),
            ("标签路由", "WPS Note 标签 / KOS 前缀"),
        ],
    ),
    "research": Scenario(
        key="research",
        label="学习",
        upstream_hint="e.g. Minerva 深度研究 / 认知升级 / 能力拓展",
        nogos=["不用猜测代替结论 (必须联网/源码验证)", "不脱离 current.goals 范围"],
        sections=[
            ("研究问题", "核心问题 / 假设"),
            ("方法", "minerva / 文献 / 实验法"),
            ("来源", "权威源 / 引用"),
        ],
    ),
    "family": Scenario(
        key="family",
        label="家庭",
        upstream_hint="e.g. 家庭管理 / 育儿 / 家务协调",
        nogos=["不暴露家庭隐私 (privacy=confidential)", "不引入外部家庭服务依赖"],
        sections=[
            ("家庭成员", "涉及成员 / 角色"),
            ("场景", "日常/教育/出行/庆祝"),
            ("隐私等级", "confidential / internal"),
        ],
    ),
    "health": Scenario(
        key="health",
        label="健康",
        upstream_hint="e.g. 个人/家庭健康追踪 / 健康习惯养成",
        nogos=["非医疗诊断 (不替代医生)", "隐私=confidential"],
        sections=[
            ("健康主体", "本人/家人"),
            ("指标", "症状/体征/习惯"),
            ("隐私等级", "confidential"),
        ],
    ),
    "finance": Scenario(
        key="finance",
        label="财务",
        upstream_hint="e.g. 个人财务规划 / 收支平衡 / 资产配置",
        nogos=["非投资建议 (合规)", "不接入银行 API (隐私+安全)"],
        sections=[
            ("财务场景", "收支/预算/资产/负债/税务/保险"),
            ("周期", "月度/季度/年度"),
            ("理财原则", "收支两条线/应急储备/风险分散"),
        ],
    ),
}


def get_scenario(key: str | None) -> Scenario | None:
    """按 key 取场景; None 或未匹配返 None (调用方回退通用模板)."""
    if not key:
        return None
    return SCENARIOS.get(key)


__all__ = ("SCENARIOS", "Scenario", "get_scenario")

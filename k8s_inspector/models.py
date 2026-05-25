"""
数据模型（巡检领域对象）。

设计目标：
- 用统一的数据结构表达“每个检查项的结果”和“整次巡检报告”
- 让输出（json/csv/html）与通知（飞书）都只依赖模型，而不是依赖各检查项的实现细节

核心对象：
- Status：检查项状态（PASS/WARN/FAIL/SKIP）
- CheckResult：单个检查项输出（摘要 + 结构化 data + 耗时）
- Report：整次巡检的汇总（含统计信息）
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
# 这个asdict函数可以将数据类转换为字典，将数据类的字段转换为字典的键值对。
# 例如：asdict(CheckResult(name="nodes", status=Status.PASS, summary="nodes are healthy"))
# 输出：{'name': 'nodes', 'status': 'PASS', 'summary': 'nodes are healthy'}

from enum import Enum
from typing import Any


class Status(str, Enum):
    """检查项状态枚举。"""
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"
    SKIP = "SKIP"


@dataclass
class CheckResult:
    """
    单个检查项结果。

    - name：检查项标识（例如 nodes/pods/control-plane）
    - status：PASS/WARN/FAIL/SKIP
    - summary：给人看的短摘要
    - data：结构化明细（便于写入 JSON/后续做统计）
    - duration_ms：该检查项耗时（毫秒）
    """
    name: str
    status: Status
    summary: str
    data: dict[str, Any] = field(default_factory=dict)
    duration_ms: int | None = None

    def to_dict(self) -> dict[str, Any]:
        """用于 JSON 序列化：将 Enum 转为字符串，并保持字段结构稳定。"""
        d = asdict(self)
        d["status"] = self.status.value
        return d


@dataclass
class Report:
    """
    一次巡检的汇总报告。

    generated_at：
      - 报告生成时间（ISO8601 字符串）
    cluster：
      - 集群标识（优先 --cluster / INSPECTOR_CLUSTER，否则回退为 in-cluster / kubeconfig / context 名）
    results：
      - 检查项结果列表
    """
    generated_at: str
    cluster: str
    results: list[CheckResult]

    # property说白了就是不将类内部的属性暴露出去，而是通过方法的方式访问，这样可以控制属性的访问权限（只读/可读写）
    @property
    def has_fail(self) -> bool:
        """是否存在 FAIL 检查项。"""
        return any(r.status == Status.FAIL for r in self.results)

    @property
    def has_warn(self) -> bool:
        """是否存在 WARN 检查项。"""
        return any(r.status == Status.WARN for r in self.results)

    def to_dict(self) -> dict[str, Any]:
        """报告序列化：包含 summary 统计与逐项 results。"""
        return {
            "generated_at": self.generated_at,
            "cluster": self.cluster,
            "summary": {
                "total": len(self.results),
                "pass": sum(1 for r in self.results if r.status == Status.PASS),
                "warn": sum(1 for r in self.results if r.status == Status.WARN),
                "fail": sum(1 for r in self.results if r.status == Status.FAIL),
                "skip": sum(1 for r in self.results if r.status == Status.SKIP),
            },
            "results": [r.to_dict() for r in self.results],
        }

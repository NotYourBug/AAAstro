"""
配置加载模块。

目的：
- 把“巡检阈值/参数”从代码中抽出来，方便不同集群/环境按需调整（也就是说阈值和一些特定的参数都写在这个模块中，换个环境需要调整的话直接改这里就好了）
- 提供合理默认值：即使不提供配置文件，也能直接运行

配置来源：
- 不传 --config：使用默认 InspectorConfig()
- 传 --config：
  - .json：使用标准库 json 解析
  - .yaml/.yml：使用 PyYAML 解析（requirements 中为可选依赖）

注意：
本模块只负责“读配置 + 合并默认值”，不做任何 K8s API 调用。
【配置文件读取 + 阈值管理】
1. 定义巡检的所有阈值（CPU 80%、内存 85%、Pod 重启 3 次等）
2. 支持从 JSON/YAML 文件读取配置
3. 没有配置文件也能正常运行（用默认值）
4. 把代码和配置分离 → 改阈值不用改代码！
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
"""
这个pathlib是Python文件处理路径的库，它提供了一些方便的方法来操作文件路径。
可以用来：
1. 读取文件
2. 判断文件是否存在
3. 获取文件后缀
4. 安全读写文件

p = Path(path)       # 把字符串路径变成 Path 对象
raw = p.read_text()  # 直接读取文件的内容，不需要再调用open函数之类的了，比较方便
suffix = p.suffix    # 获取文件的后缀
"""
from typing import Any


@dataclass
class Thresholds:
    """巡检阈值集合（可通过配置文件覆盖）。"""
    node_cpu_utilization: float = 0.80
    node_memory_utilization: float = 0.85
    pod_restart_count: int = 3
    warning_events_max: int = 50
    """
    以后如果需要调整告警标准，只需要改这里，不用动业务代码。
    参数说明：
    node_cpu_utilization：节点 CPU 占用率阈值（默认 0.80）
    node_memory_utilization：节点内存占用率阈值（默认 0.85）
    pod_restart_count：Pod 重启次数阈值（默认 3）
    warning_events_max：最大告警事件数（默认 50）
    """


@dataclass
class InspectorConfig:
    """
    巡检配置（这里是总配置，囊括了上面的阈值集合），包括巡检阈值（threshold）、控制面板的命名空间、允许的Noschedule污点列表。

    control_plane_namespace:
      - 控制平面组件通常位于 kube-system，但也可能在其他 namespace（或以静态 Pod 形式存在）

    allow_noschedule_taints:
      - 有些 NoSchedule taint（例如控制平面节点）是合理的，不希望被巡检当成告警
      - 这里存放允许的 taint key 列表
    """
    thresholds: Thresholds = field(default_factory=Thresholds)
    # 给配置类创建一个阈值对象，并且每次创建配置时，都自动生成一个全新的阈值对象，不会共用、不会污染。
    """field是dataclass的高级属性设置器
    默认情况下dataclass是只能写：x:int=10
    但是说如果你想：
        设置动态默认值
        某个字段不参与初始化
        某个字段不打印
        某个字段不参与比较
    就必须使用field()，他就是给字段加高级功能的。
    """
    control_plane_namespace: str = "kube-system"
    allow_noschedule_taints: list[str] = field(default_factory=list)
    # default_factory=类名()，每次创建对象的时候，都会生成一个全新的实例。


def _deep_get(d: dict[str, Any], path: list[str], default: Any) -> Any:
    """
    从嵌套 dict 中安全地读取字段。

    - path: 例如 ["thresholds", "node_cpu_utilization"]
    - 若任意层不存在，则返回 default
    """
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    # 从多重嵌套的配置文件里安全拿值，不会报错！
    # 比如：data["threshold"]["node_cpu_utilization"]
    # 如果配置文件没有写，直接访问就会报错
    # 用_deep_get就会自动返回默认值
    return cur



def load_config(path: str | None) -> InspectorConfig:
    """
    加载配置文件并与默认配置合并。

    返回：
    - InspectorConfig：用于巡检流程（checks）
    """
    cfg = InspectorConfig()
    # 如果没传入配置文件的就用咱们默认的巡检配置
    if not path:
        return cfg

    p = Path(path)
    raw = p.read_text(encoding="utf-8")
    data: dict[str, Any]

    # 如果传入的是yaml文件的话需要导入PyYAML模块来解析配置文件
    if p.suffix.lower() in {".yaml", ".yml"}:
        try:
            import yaml  # type: ignore
        except Exception as e:  # pragma: no cover
            raise RuntimeError("读取 YAML 配置需要安装 PyYAML") from e
        data = yaml.safe_load(raw) or {}
    else:
        # 不然的话默认就是JSON格式，直接进行解析
        data = json.loads(raw) if raw.strip() else {}

    # 从多层嵌套的配置文件中，找出指定字段，如果说没找到的话，就返回先前配置的默认字段值
    cfg.control_plane_namespace = _deep_get(
        data, ["control_plane_namespace"], cfg.control_plane_namespace
    )
    cfg.allow_noschedule_taints = _deep_get(
        data, ["allow_noschedule_taints"], cfg.allow_noschedule_taints
    )

    t = cfg.thresholds
    # 这里才是多层嵌套查找的用处体现，node_cpu_utilization/node_memory_utilization都是嵌套在threshold中的
    t.node_cpu_utilization = float(
        _deep_get(data, ["thresholds", "node_cpu_utilization"], t.node_cpu_utilization)
    )
    t.node_memory_utilization = float(
        _deep_get(
            data,
            ["thresholds", "node_memory_utilization"],
            t.node_memory_utilization,
        )
    )
    t.pod_restart_count = int(
        _deep_get(data, ["thresholds", "pod_restart_count"], t.pod_restart_count)
    )
    t.warning_events_max = int(
        _deep_get(data, ["thresholds", "warning_events_max"], t.warning_events_max)
    )
    """
    所以总结来说逻辑很简单，就是说判断传没传入配置文件，没传，就直接返回原来的默认巡检配置
    传了，就读文件，拿到新的巡检值，并将其重新赋值给指定变量。
    """

    return cfg

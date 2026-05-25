# K8s 集群自动化巡检（Python + Kubernetes SDK）

基于 instruction.txt 的巡检维度（基础设施/控制平面/数据平面）实现的可落地工具，支持：

- 自动化巡检：Node / Pod / PVC / Event / 控制平面组件健康
- 报告输出：JSON / CSV / HTML
- 告警推送：飞书群机器人 Webhook（仅推送失败项，可选）
- 执行方式：本地运行 / 容器化 + CronJob

## 快速开始（本地）

1) 安装依赖

```bash
pip install -r requirements.txt
```

2) 使用本地 kubeconfig 执行

```bash
python -m k8s_inspector.cli run --kubeconfig ~/.kube/config --formats json,html --output-dir ./out --profile full
```

3) 集群内执行（Pod/CronJob 中）

```bash
INSPECTOR_CLUSTER=prod python -m k8s_inspector.cli run --in-cluster --formats json --output-dir /data/out --profile 5m
```

## 失败项推送（飞书）

```bash
python -m k8s_inspector.cli run --kubeconfig ~/.kube/config --feishu-webhook "https://open.feishu.cn/open-apis/bot/v2/hook/xxx"
```

## 配置文件

默认无需配置文件。你也可以传入 JSON 或 YAML（YAML 依赖 PyYAML）：

```bash
python -m k8s_inspector.cli run --config ./config.yaml
```

示例（config.yaml）：

```yaml
thresholds:
  node_cpu_utilization: 0.80
  node_memory_utilization: 0.85
  pod_restart_count: 3
  warning_events_max: 50
control_plane_namespace: kube-system
allow_noschedule_taints:
  - "node-role.kubernetes.io/control-plane"
```

## 容器化与 CronJob

部署清单在 deploy/：

- deploy/kustomization.yaml：方案A一键部署（集群内 Prometheus+Alertmanager+Pushgateway+Bridge+RBAC+CronJobs）
- deploy/storage/：PV/PVC 示例（用于巡检结果落盘归档）
- deploy/rbac.yaml：最小化 list/watch 权限 + /healthz 非资源 URL 访问
- deploy/cronjob.yaml：示例 CronJob（按需修改镜像、参数、频率、输出卷）
- deploy/cronjobs-by-frequency.yaml：按 5m/10m/hourly/daily 拆分的多 CronJob 示例

### 方案A（推荐，集群内一键部署）

```bash
kubectl apply -f deploy/secret-feishu.example.yaml
kubectl apply -k deploy
```

应用前请先修改以下占位项：

- `your-registry/k8s-inspector:0.1.0`（巡检镜像地址：在 deploy/cronjobs-by-frequency.yaml 与 deploy/alertmanager-feishu-bridge.yaml 中）
- `deploy/secret-feishu.example.yaml`：
  - `app_id`
  - `app_secret`
  - `alert_user_open_ids`
- `INSPECTOR_CLUSTER`：集群标识（建议必填，用于指标/告警/群名区分集群；deploy/cronjobs-by-frequency.yaml / deploy/cronjob.yaml 默认是占位符 YOUR_CLUSTER_NAME，部署前必须替换）

## 退出码

- 0：无 FAIL（可能包含 WARN）
- 2：存在 FAIL
- 1：仅当传入 --fail-on warn 且存在 WARN 时

## 执行频率（profile）

- 5m：数据平面关键项（Node Ready/污点、Pod 异常、Pending PVC），不拉取 Metrics，降低 API 压力
- 10m：控制平面（/healthz + 控制平面组件 Pod）
- hourly：基础设施关键项（控制平面 + Node（含 Metrics）+ Pod + PVC）
- daily/full：全量（hourly + Warning Event + kube-system 关键组件）

## Prometheus + Grafana（可观测性增强）

本项目支持将巡检结果推送到 Pushgateway，再由 Prometheus 抓取，Grafana 展示大盘。

1) 部署 Pushgateway（示例）

```bash
kubectl apply -f deploy/pushgateway.yaml
```

2) 配置 Prometheus 抓取 Pushgateway

Prometheus 原生配置可参考：

- deploy/prometheus-scrape-pushgateway-snippet.yaml

3) 让巡检 CronJob 推送指标

在 CronJob 的 env 中配置：

- PUSHGATEWAY_URL：例如 http://pushgateway.kube-system.svc:9091
- PUSHGATEWAY_JOB：默认 k8s-inspector

或本地运行时传参：

```bash
python -m k8s_inspector.cli run --kubeconfig ~/.kube/config --profile full --pushgateway-url http://127.0.0.1:9091
```

4) 导入 Grafana Dashboard

Grafana 导入 JSON：

- deploy/grafana-dashboard-k8s-inspector.json

## Alertmanager（告警治理）

如果你已经使用 Prometheus + Alertmanager，可以把“脚本直推飞书”升级为“Prometheus 规则 → Alertmanager 聚合/去重/抑制 → 飞书”。

1) 部署飞书告警桥接服务（接收 Alertmanager webhook，再转发到飞书）

```bash
kubectl apply -f deploy/alertmanager-feishu-bridge.yaml
```

在 deploy/alertmanager-feishu-bridge.yaml 中：
- 若使用群机器人 webhook（仅推送到固定群），配置 FEISHU_WEBHOOK
- 若需要自动拉群/拉人/解决后改群名（推荐用于 P0/P1 告警），配置应用机器人相关环境变量：
  - FEISHU_MODE=app
  - FEISHU_APP_ID / FEISHU_APP_SECRET
  - FEISHU_ALERT_USER_OPEN_IDS（逗号分隔 open_id）
  - FEISHU_CHAT_NAME_TEMPLATE（默认 K8s告警-{cluster}-{alertname}）
  - FEISHU_CHAT_RESOLVED_SUFFIX（默认 [已解决]）
  - FEISHU_CHAT_STATE_PATH（默认 /data/state/feishu_chats.json）
- 为了让飞书告警更“可读”，bridge 会尝试读取巡检输出目录下的 latest report.json，把 CheckResult.summary/data 做摘要后附加到卡片中：
  - INSPECTOR_REPORT_DIR（默认 /data/out）
  - 需要把巡检输出用的 PVC（k8s-inspector-out）挂载给 bridge（deploy/alertmanager-feishu-bridge.yaml 已默认只读挂载到 /data/out）

2) 配置 Prometheus 告警规则

Prometheus 规则文件：

- deploy/prometheus-alert-rules-k8s-inspector.yaml

3) 配置 Alertmanager 路由与抑制规则

Alertmanager 配置片段：

- deploy/alertmanager-config-snippet.yaml

该配置包含：
- 按 cluster/profile/check 分组聚合
- 对 K8sInspectorStale 做抑制：当巡检未运行时，避免旧数据触发重复 FAIL/WARN 告警

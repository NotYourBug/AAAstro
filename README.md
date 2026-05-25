# K8s 集群自动化巡检（Python + Kubernetes SDK）
作为 k8s SRE，集群巡检的核心目标是**提前发现潜在故障、验证集群健康状态、确保业务稳定性**，需覆盖「基础设施层→控制平面→数据平面→业务层→可观测性」全维度，同时兼顾**自动化执行**和**人工复核**。基于以下的巡检维度（基础设施/控制平面/数据平面）实现的可落地工具，支持：

+ 自动化巡检：Node / Pod / PVC / Event / 控制平面组件健康
+ 报告输出：JSON / CSV / HTML
+ 告警推送：飞书群机器人 Webhook（仅推送失败项，可选）
+ 执行方式：本地运行 / 容器化 + CronJob

### 一、巡检整体框架
| **<font style="color:rgb(0, 0, 0);">巡检维度</font>** | **<font style="color:rgb(0, 0, 0);">核心目标</font>** | **<font style="color:rgb(0, 0, 0);">执行方式</font>** | **<font style="color:rgb(0, 0, 0);">执行频率</font>** |
| --- | --- | --- | --- |
| <font style="color:rgb(0, 0, 0);">基础设施层</font> | <font style="color:rgb(0, 0, 0);">节点 / 网络 / 存储可用性</font> | <font style="color:rgb(0, 0, 0);">自动化 </font> | <font style="color:rgb(0, 0, 0);">每小时（关键项）、每日（全量）</font> |
| <font style="color:rgb(0, 0, 0);">控制平面</font> | <font style="color:rgb(0, 0, 0);">apiserver/etcd/scheduler 等组件健康</font> | <font style="color:rgb(0, 0, 0);">自动化</font> | <font style="color:rgb(0, 0, 0);">每 10 分钟</font> |
| <font style="color:rgb(0, 0, 0);">数据平面</font> | <font style="color:rgb(0, 0, 0);">Node/Pod / 存储卷健康</font> | <font style="color:rgb(0, 0, 0);">自动化</font> | <font style="color:rgb(0, 0, 0);">每 5 分钟</font> |


### 二、核心巡检项（附检查方法 / API / 工具）
#### 基础设施层巡检
| **<font style="color:rgb(0, 0, 0);">检查项</font>** | **<font style="color:rgb(0, 0, 0);">检查标准</font>** | **<font style="color:rgb(0, 0, 0);">检查方法 / 工具</font>** |
| --- | --- | --- |
| <font style="color:rgb(0, 0, 0);">节点状态</font> | <font style="color:rgb(0, 0, 0);">所有 Node 状态为 Ready，无 NotReady/Taint（业务节点无 NoSchedule 污点）</font> | <font style="color:rgb(0, 0, 0);">kubectl get nodes / CoreV1Api.list_node () / 代码过滤 Ready 状态</font> |
| <font style="color:rgb(0, 0, 0);">节点系统资源</font> | <font style="color:rgb(0, 0, 0);">CPU 使用率 < 80%、内存使用率 < 85%、磁盘使用率 < 85%（/var/lib/kubelet 目录）</font> | <font style="color:rgb(0, 0, 0);">kubectl top nodes / Metrics API /node-exporter 监控 / df -h 检查磁盘</font> |
| <font style="color:rgb(0, 0, 0);">节点网络</font> | <font style="color:rgb(0, 0, 0);">节点间网络互通（6443/10250/10255 端口通），CNI 插件（calico/flux）运行正常</font> | <font style="color:rgb(0, 0, 0);">ping/telnet / kubectl get pods -n kube-system -l k8s-app=calico-node</font> |
| <font style="color:rgb(0, 0, 0);">存储后端</font> | <font style="color:rgb(0, 0, 0);">存储类（StorageClass）可用，PV 绑定状态正常，无 Pending PVC</font> | <font style="color:rgb(0, 0, 0);">kubectl get sc/pv/pvc / CoreV1Api.list_pvc () 过滤 phase=Pending</font> |


#### 控制平面巡检
| **<font style="color:rgb(0, 0, 0);">检查项</font>** | **<font style="color:rgb(0, 0, 0);">检查标准</font>** | **<font style="color:rgb(0, 0, 0);">检查方法 / 工具</font>** |
| --- | --- | --- |
| <font style="color:rgb(0, 0, 0);">apiserver</font> | <font style="color:rgb(0, 0, 0);">副本数正常（多主集群≥2），Pod Running，API 响应时间 < 500ms，无 5xx 错误</font> | <font style="color:rgb(0, 0, 0);">kubectl get pods -n kube-system -l component=kube-apiserver / kubectl get --raw=/healthz / Prometheus 监控 apiserver_request_duration_seconds</font> |
| <font style="color:rgb(0, 0, 0);">etcd 集群</font> | <font style="color:rgb(0, 0, 0);">集群健康（member 健康数 = 副本数），磁盘使用率 < 80%，无 leader 切换频繁（<1 次 / 天）</font> | <font style="color:rgb(0, 0, 0);">kubectl -n kube-system exec etcd-master -- etcdctl endpoint health /etcd 监控（etcd_disk_usage_percent）</font> |
| <font style="color:rgb(0, 0, 0);">controller-manager</font> | <font style="color:rgb(0, 0, 0);">Pod Running，无重启，核心控制器（如 deployment/pod 控制器）无报错</font> | <font style="color:rgb(0, 0, 0);">kubectl get pods -n kube-system -l component=kube-controller-manager / 查看日志kubectl logs -n kube-system kube-controller-manager</font> |
| <font style="color:rgb(0, 0, 0);">scheduler</font> | <font style="color:rgb(0, 0, 0);">Pod Running，无重启，调度成功率 > 99%（无 Pending Pod 因调度失败）</font> | <font style="color:rgb(0, 0, 0);">kubectl get pods -n kube-system -l component=kube-scheduler / 检查 Event 中FailedScheduling事件</font> |


#### 数据平面巡检
| **<font style="color:rgb(0, 0, 0);">检查项</font>** | **<font style="color:rgb(0, 0, 0);">检查标准</font>** | **<font style="color:rgb(0, 0, 0);">检查方法 / 工具</font>** |
| --- | --- | --- |
| <font style="color:rgb(0, 0, 0);">Pod 健康状态</font> | <font style="color:rgb(0, 0, 0);">业务 Pod Running，重启次数 < 3 次 / 天，无 CrashLoopBackOff/Error 状态</font> | <font style="color:rgb(0, 0, 0);">kubectl get pods --all-namespaces / CoreV1Api.list_pod_for_all_namespaces () 过滤 phase=Failed/Unknown</font> |
| <font style="color:rgb(0, 0, 0);">Node 组件健康</font> | <font style="color:rgb(0, 0, 0);">kubelet/kube-proxy Pod Running，节点日志无 ERROR 级报错</font> | <font style="color:rgb(0, 0, 0);">kubectl get pods -n kube-system -l k8s-app=kubelet / journalctl -u kubelet</font> |
| <font style="color:rgb(0, 0, 0);">事件（Event）</font> | <font style="color:rgb(0, 0, 0);">无大量 Warning/Error 级 Event（<5 个 / 小时），无重复报错（如 ImagePullBackOff）</font> | <font style="color:rgb(0, 0, 0);">kubectl get events --all-namespaces --field-selector type=Warning / Event API 监听</font> |


基于以上的巡检维度（基础设施/控制平面/数据平面）实现的可落地工具，支持：

+ 自动化巡检：Node / Pod / PVC / Event / 控制平面组件健康
+ 报告输出：JSON / CSV / HTML
+ 告警推送：飞书群机器人 Webhook（仅推送失败项，可选）
+ 执行方式：本地运行 / 容器化 + CronJob

以下是一套可落地、可扩展的 K8s 集群巡检方案，包含巡检维度、核心检查项、工具化实现、执行频率和故障闭环。

## 目录
+ [项目架构](#项目架构)
+ [快速开始（本地）](#快速开始本地)
+ [容器化与集群部署](#容器化与集群部署)
+ [存储与留存策略（PV/PVC）](#存储与留存策略pvpvc)
+ [Prometheus + Grafana（可观测性增强）](#prometheus--grafana可观测性增强)
+ [Alertmanager（告警治理）](#alertmanager告警治理)
+ [安全建议](#安全建议)
+ [常见问题](#常见问题)

## 项目架构
本项目提供两条常见落地路径：

+ **轻量模式（脚本 + 报告）**：CronJob/本地运行 → 输出 JSON/CSV/HTML →（可选）飞书群机器人推送摘要
+ **生产模式（可观测 + 告警治理）**：CronJob → Pushgateway → Prometheus（规则）→ Alertmanager（聚合/抑制）→ Bridge（飞书通知/自动拉群）

核心组件：

+ `k8s_inspector/cli.py`：巡检任务入口（短任务）
+ `k8s_inspector/checks.py`：巡检规则
+ `k8s_inspector/report.py`：报告输出（JSON/CSV/HTML）
+ `k8s_inspector/metrics.py`：Pushgateway 指标推送
+ `k8s_inspector/alertmanager_bridge.py`：接收 Alertmanager webhook 的桥接服务（常驻）
+ `deploy/`：Kustomize 部署清单（RBAC/CronJobs/监控/告警/存储）

## 目录结构
+ `k8s_inspector/`：核心代码（巡检/报告/指标/告警桥接）
+ `deploy/`：集群内部署（Kustomize）
+ `deploy/storage/`：PV/PVC 示例（NFS RWX）
+ `tests/`：单元测试
+ `README-MCP.md` ：项目简介

## 快速开始（本地）
1. 安装依赖

```bash
pip install -r requirements.txt
```

1. 使用本地 kubeconfig 执行

```bash
python -m k8s_inspector.cli run --kubeconfig ~/.kube/config --formats json,html --output-dir ./out --profile full
```

1. 集群内执行（Pod/CronJob 中）

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

```plain
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

+ deploy/kustomization.yaml：方案A一键部署（集群内 Prometheus+Alertmanager+Pushgateway+Bridge+RBAC+CronJobs）
+ deploy/storage/：PV/PVC 示例（用于巡检结果落盘归档）
+ deploy/rbac.yaml：最小化 list/watch 权限 + /healthz 非资源 URL 访问
+ deploy/cronjob.yaml：示例 CronJob（按需修改镜像、参数、频率、输出卷）
+ deploy/cronjobs-by-frequency.yaml：按 5m/10m/hourly/daily 拆分的多 CronJob 示例

### 方案A（推荐，集群内一键部署）
部署前准备：

1. 先构建并推送镜像（示例）

```bash
docker build -t your-registry/k8s-inspector:0.1.0 .
docker push your-registry/k8s-inspector:0.1.0
```

1. 修改部署占位符（必须）
+ `your-registry/k8s-inspector:0.1.0`：替换为你的镜像仓库地址
+ `INSPECTOR_CLUSTER=YOUR_CLUSTER_NAME`：替换为你的真实集群名（避免多集群指标/告警混淆）
+ 飞书配置：按需选择 webhook 或应用机器人模式

```bash
kubectl apply -f deploy/secret-feishu.example.yaml
kubectl apply -k deploy
```

应用前请先修改以下占位项：

+ `your-registry/k8s-inspector:0.1.0`（巡检镜像地址：在 deploy/cronjobs-by-frequency.yaml 与 deploy/alertmanager-feishu-bridge.yaml 中）
+ `deploy/secret-feishu.example.yaml`：
    - `app_id`
    - `app_secret`
    - `alert_user_open_ids`
+ `INSPECTOR_CLUSTER`：集群标识（建议必填，用于指标/告警/群名区分集群；deploy/cronjobs-by-frequency.yaml / deploy/cronjob.yaml 默认是占位符 YOUR_CLUSTER_NAME，部署前必须替换）

## 退出码
+ 0：无 FAIL（可能包含 WARN）
+ 2：存在 FAIL
+ 1：仅当传入 --fail-on warn 且存在 WARN 时

## 执行频率（profile）
+ 5m：数据平面关键项（Node Ready/污点、Pod 异常、Pending PVC），不拉取 Metrics，降低 API 压力
+ 10m：控制平面（/healthz + 控制平面组件 Pod）
+ hourly：基础设施关键项（控制平面 + Node（含 Metrics）+ Pod + PVC）
+ daily/full：全量（hourly + Warning Event + kube-system 关键组件）

## 存储与留存策略（PV/PVC）
本项目默认通过 PV/PVC 持久化巡检产物，并按频率做分层留存，避免高频任务导致存储无限增长：

+ **高频（5m/10m）**：写入 `latest/<profile>/report.json`，覆盖写（并可配置 `--persist-level issue` 仅在 WARN/FAIL 时落盘）
+ **低频（hourly/daily）**：写入 `archive/<profile>/<时间目录>/report.json`，用于追溯与复盘
+ **定期清理**：`cleanup` CronJob 负责删除过期归档目录（hourly 7 天，daily 30 天，默认可改）

对应文件：

+ PV/PVC 示例：`deploy/storage/`
+ 归档与清理：`deploy/cleanup-cronjob.yaml` + `k8s-inspector cleanup` 子命令

## Prometheus + Grafana（可观测性增强）
本项目支持将巡检结果推送到 Pushgateway，再由 Prometheus 抓取，Grafana 展示大盘。

1. 部署 Pushgateway（示例）

```bash
kubectl apply -f deploy/pushgateway.yaml
```

2. 配置 Prometheus 抓取 Pushgateway

Prometheus 原生配置可参考：

+ deploy/prometheus-scrape-pushgateway-snippet.yaml
3. 让巡检 CronJob 推送指标

在 CronJob 的 env 中配置：

+ PUSHGATEWAY_URL：例如 [http://pushgateway.kube-system.svc:9091](http://pushgateway.kube-system.svc:9091)
+ PUSHGATEWAY_JOB：默认 k8s-inspector

或本地运行时传参：

```bash
python -m k8s_inspector.cli run --kubeconfig ~/.kube/config --profile full --pushgateway-url http://127.0.0.1:9091
```

4. 导入 Grafana Dashboard

Grafana 导入 JSON：

+ deploy/grafana-dashboard-k8s-inspector.json

## Alertmanager（告警治理）
如果你已经使用 Prometheus + Alertmanager，可以把“脚本直推飞书”升级为“Prometheus 规则 → Alertmanager 聚合/去重/抑制 → 飞书”。

1. 部署飞书告警桥接服务（接收 Alertmanager webhook，再转发到飞书）

```bash
kubectl apply -f deploy/alertmanager-feishu-bridge.yaml
```

在 deploy/alertmanager-feishu-bridge.yaml 中：

+ 若使用群机器人 webhook（仅推送到固定群），配置 FEISHU_WEBHOOK
+ 若需要自动拉群/拉人/解决后改群名（推荐用于 P0/P1 告警），配置应用机器人相关环境变量：
    - FEISHU_MODE=app
    - FEISHU_APP_ID / FEISHU_APP_SECRET
    - FEISHU_ALERT_USER_OPEN_IDS（逗号分隔 open_id）
    - FEISHU_CHAT_NAME_TEMPLATE（默认 K8s告警-{cluster}-{alertname}）
    - FEISHU_CHAT_RESOLVED_SUFFIX（默认 [已解决]）
    - FEISHU_CHAT_STATE_PATH（默认 /data/state/feishu_chats.json）
+ 为了让飞书告警更“可读”，bridge 会尝试读取巡检输出目录下的 latest report.json，把 CheckResult.summary/data 做摘要后附加到卡片中：
    - INSPECTOR_REPORT_DIR（默认 /data/out）
    - 需要把巡检输出用的 PVC（k8s-inspector-out）挂载给 bridge（deploy/alertmanager-feishu-bridge.yaml 已默认只读挂载到 /data/out）

2. 配置 Prometheus 告警规则
   Prometheus 规则文件：deploy/prometheus-alert-rules-k8s-inspector.yaml

3. 配置 Alertmanager 路由与抑制规则
   Alertmanager 配置片段：deploy/alertmanager-config-snippet.yaml
   该配置包含：

   * 按 cluster/profile/check 分组聚合

   + 对 K8sInspectorStale 做抑制：当巡检未运行时，避免旧数据触发重复 FAIL/WARN 告警

说明：为了降低“Pushgateway 残留旧值”导致的误报，本仓库的告警规则已对关键告警增加“新鲜度约束”（last_run 在窗口内才允许触发）。

## 安全建议
+ **不要提交真实密钥到 GitHub**：`deploy/secret-feishu.example.yaml` 仅做示例，请自行在私有环境创建真实 Secret。
+ **最小权限 RBAC**：优先使用 `deploy/rbac.yaml` 的只读权限；不要给 `cluster-admin`。
+ **多集群强制区分**：部署前必须设置 `INSPECTOR_CLUSTER`（默认占位符 `YOUR_CLUSTER_NAME`）。
+ **证书校验**：测试环境可临时关闭；生产环境建议开启 K8s/飞书 API 的证书校验与网络隔离。

## 常见问题
+ `kubectl apply -f kustomization.yaml` 报 `no matches for kind "Kustomization"`
    - 用 `kubectl apply -k deploy`。
+ CronJob `BackoffLimitExceeded` 且 `kubectl logs` 为空
    - 常见原因是“退出码非 0”导致 Job 标记失败；项目支持 `INSPECTOR_EXIT_MODE=always0`（推荐配合 Prometheus 告警）。
+ Pushgateway/Prometheus 有数据但飞书无消息
    - 按链路排查：Prometheus 规则 → Alertmanager receiver → Bridge `/alert` 返回码与日志 → 飞书配置与权限。
+ `K8sInspectorHasFail` 看似误报
    - 先看 `K8sInspectorStale` 是否同时触发；本仓库规则已增加 last_run 新鲜度约束，避免旧值误报。

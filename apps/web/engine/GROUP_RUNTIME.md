# 策略组运行服务

`nautilus-group-supervisor.service` 是与 BFF 分开的运行服务，拥有独立的 cgroup、资源上限和
状态目录；BFF 重启时策略照常运行。它启动 root 注册文件中发布的策略组：

| 组 | worker | 运行方式 |
|---|---|---|
| 基线组 | `group_worker.py` | 现货 CASH 1x Nautilus Sandbox，冻结信号的一次初始配置 |
| 模型组 | `model_group_worker.py` | 已发布模型版本的 shadow 运行（见 MODEL_RUNTIME.md） |
| 实盘组 | `live_hft_worker.py`、`live_model_worker.py`、`live_alpha_worker.py` | OKX 实盘，凭据由 worker 从 root 管理的密钥库读取 |

上传区内容只作为来源证明；运行的代码都是 root 预装并按 hash 校验的版本。

## 基线组

`group_worker.py` 复用已审核的 `SectorPaper` 冻结权重规则，以及 hardened engine 的风控、对账、
告警、流式落盘和控制 socket。`marketTransport` 由 root 发布：默认 `native_ws`；
`public_rest_l2` 复用已审核策略模块中的 `PublicOKXFactory` / `SnapshotOKXDataClient`，TLS 校验
照常。撮合采用 L2 Sandbox，单边手续费 10 bps。manifest、status、final 和运行 API 记录实际使用
的传输方式，运行期间保持不变。

该组是固定信号的一次初始配置；日线研究的 5 个共同交易日重算尚未接入。新的 Alpha 先发布独立
审核配置，再获得启动能力。

## 部署目录

- `/usr/local/lib/montlok-groups/server/group_runtime.py`：独立服务及 BFF 客户端。
- `/usr/local/lib/montlok-groups/engine/`：各组 worker。
- `/usr/local/lib/montlok-groups/hardened/`：已验证 `b382994cde` 的 `alerts.py`、`commands.py`、`control.py`、`health.py`、`node_config.py`、`run.py`、`settings.py`。
- `/usr/local/lib/montlok-groups/releases/baseline_sector.py`：发布时冻结的 `sector_rotation_paper.py` 副本。
- `/etc/montlok-groups/registry.json`、`baseline.settings.json`、`baseline.signals.json`：root 管理的审核清单。
- `/www/nautilus/group-runs/`：`nautilus` 可写；保存 `runs.sqlite` 以及每个实例的请求、日志、控制 socket、状态、曲线和交易报表。

程序、Python 解释器、发布清单及其父目录归 root 所有，group/other 只读；解释器可以是 root 管理
的 venv 符号链接。BFF 只连接 `supervisor.sock`，进程都由 supervisor 启动。

## 注册格式

```json
{
  "version": 1,
  "pythonPath": "/opt/montlok-runtime/bin/python",
  "workerPath": "/usr/local/lib/montlok-groups/engine/group_worker.py",
  "groups": [{
    "id": "baseline",
    "name": "四板块 60/40",
    "enabled": true,
    "environment": "sandbox",
    "marketTransport": "native_ws",
    "defaultBudgetUsdt": "2000.00",
    "maxBudgetUsdt": "2000.00",
    "maxDurationSeconds": 86400,
    "runtimePath": "/usr/local/lib/montlok-groups/hardened",
    "settingsPath": "/etc/montlok-groups/baseline.settings.json",
    "settingsSha256": "实际文件SHA256",
    "signalsPath": "/etc/montlok-groups/baseline.signals.json",
    "signalsSha256": "实际文件SHA256",
    "strategyPath": "/usr/local/lib/montlok-groups/releases/baseline_sector.py",
    "strategySha256": "实际文件SHA256"
  }]
}
```

`baseline.settings.json` 使用 hardened `Settings` 格式：`environment=sandbox`、
`instrument_types=["SPOT"]`、`strategies=[]`，品种与冻结信号一致，每个品种配置正整数
`risk.max_notional_per_order`；保留告警 webhook、capture 和监控设置，OKX 密钥字段留空。运行器
覆盖本次的 `state_dir`、`sandbox_balance`，并开启 capture。

信号文件为 `execution=nautilus_sandbox`、`account_type=CASH`、`leverage=1`，每个 `*-USDT.OKX`
权重 0–10%，总权重不超过 100%。已发布文件或注册配置变化后，旧的确认请求失效；新版本需要重新
加载 supervisor，已有实例的停止操作按实例身份进行。

### 模型组：`modelRuntime`

根注册文件中的 `modelRuntime` 决定模型组的代码路径、设备、guard 和 runner 路径；HTTP 请求只
选择已发布的组、预算和时长：

```json
{
  "storePath": "/www/nautilus/operator/state/models",
  "workerPath": "/usr/local/lib/montlok-groups/engine/model_group_worker.py",
  "pythonPath": "/www/nautilus/venv/bin/python",
  "guardBinary": "/usr/local/lib/montlok-groups/bin/model-inference-guard",
  "runnerRoot": "/usr/local/lib/montlok-model-runners",
  "settingsPath": "/etc/montlok-groups/model-shadow.settings.json",
  "settingsSha256": "REPLACE_WITH_REVIEWED_SETTINGS_SHA256",
  "runtimePath": "/usr/local/lib/montlok-groups/hardened",
  "mode": "shadow",
  "maxBudgetUsdt": "100.00",
  "maxDurationSeconds": 3600,
  "maxConcurrentRuns": 2,
  "contractKey": {"rdt4quant_v1": "crypto:BTC-USDT"},
  "inferenceBackend": "montlok"
}
```

| 字段 | 作用 |
|---|---|
| `inferenceBackend` | GRU 版本的推理后端：`torch`（默认）或 `montlok`。设为 `montlok` 时，worker 以 `--backend montlok` 启动，核对该参数与注册文件一致，并在启动前与 PyTorch 对拍。`rdt4quant_cpu_v2` 版本使用 montlok 内核，其他 RDT 版本使用 PyTorch。 |
| `contractKey` | RDT 选择已发布且已接通行情的 contract：`crypto:BTC-USDT` 或 `token_hour:XNVDA`。GRU 对应 BTC-USDT。输出头由清单的 `selectedHeadByDomain` 决定。 |
| `maxConcurrentRuns` | 默认 2（1–16），统计所有存活的模型实例（含启动中和停止中），与每个版本的单实例限制分别计算。槽位用满时节点 ready 保持 true，新启动能力为 false，并显示占用数量；基线组使用自己的槽位。 |
| `mode` | `shadow`。 |

每个已发布的不可变版本对应一个组 `model-<manifestSha256 前 16 位>`。新实例从当前发布指针所选
的版本启动；已有实例保留启动时的权重、清单和命令行，回滚只影响之后的新实例。root settings 为
目标执行品种设置正整数单笔金额上限。

状态轮询读取发布索引和缓存的节点能力；预检和启动时重新校验 root 程序、guard、settings、主机
预装 runner 以及发布清单、权重和来源文件的 hash。节点能力包括 PyTorch、NumPy、Nautilus、
montlok、Python 原生数据桥，以及 RDT GPU 版本所需的 CUDA BF16 与 Mamba3；缺项时启动能力为
false，状态给出原因。worker 报告 warmup 完成、`marketReady=true`（原生行情已投递、quote
新鲜、模型输入与目标有效）且行情与执行均已连接后，实例显示 running。模型进程和 guard 在目标
主机本地运行。

### 实盘组

实盘配置（注册项 `configPath` 指向的 JSON）由 `live_hft_worker.registered()` 校验。可选
`pollIdleMicros` 为 0–1000 的整数，默认 1000：主循环在一次 `poll` 没有处理到事件时等待的
微秒数；0 表示持续轮询，行情到策略的延迟最低，同时占用一个核心。状态与视图文件每 250 ms 汇总
一次，已成交、撤销、拒绝或过期的订单只读取一次。

## Supervisor 接口

`GroupRuntimeClient(socket_path)` 提供四个 async 方法：

- `status(group_id=None)`：`{available, groups:[{groupId, name, mode, environment, ready, runId, supportedActions, capabilities, defaultBudgetUsdt, maxBudgetUsdt, maxDurationSeconds, runs}]}`。
- `prepare({groupId, action, budgetUsdt?, durationSeconds?, runId?})`：返回 `{request, mode, groupName, effect, signalVersion, strategyVersion}`。`request` 是需要绑定确认票据的规范化请求，含 `registryVersion`。
- `execute(request, operation_id)`：返回实际的启动或控制结果。同一 `operation_id` 的相同请求只执行一次；复用编号提交不同请求会被拒绝；结果未知时返回 `unknown`，之后用 `receipt` 查询。
- `receipt(operation_id)`：查询该操作的持久回执。supervisor 到 worker 的 `halt/reduce/resume` 使用同一个编号；超时与断链返回 `receiptStatus=unknown`，表示结果待查。worker 在执行前后各写一次 SQLite 日志，supervisor 重启后仍可查询；worker 停止后从已存日志读取。运行状态以最新事件为准。

BFF 的 `GET /api/operations/{id}` 只读查询回执：记录了 operator 归属的回执，同一用户重新登录后
可读；早期无归属的回执限于原会话。`POST /api/execute` 的重放限于原会话。回执完成表示控制步骤
完成：`stop` 持久化 stopRequested 后发送 SIGTERM，回执可以是 completed 而实例仍为 stopping，
进程退出且报告齐全后实例成为 stopped。关闭控制服务时先停收新的写请求，等已接受的操作完成后
再停止引擎。

动作为 `start | halt | reduce | resume | stop`。启动需要明确的虚拟资金和最长时长；其他动作带
supervisor 创建的 `runId`。BFF 在 `prepare/execute` 前验证管理员、会话、CSRF 和一次性确认票据。
`status.runs[].runDir` 供服务端读取图表，输出到浏览器前可移除绝对路径。

`halt` 暂停并撤销该实例挂单；`reduce` 只允许减仓；`resume` 恢复该实例；`stop` 结束实例进程并
输出报表，账户仓位保持原样。信号只发给 PID、启动时间和命令行都与记录一致的实例进程。

## 实例状态

- `starting`：子进程已创建，行情尚待确认。收到 PID 匹配的控制响应且 data/exec 均已连接时为 `running`。
- `stopping` → `stopped`：`stop` 先返回 `stopping`，进程干净退出后为 `stopped`；超时未退出时保持 `stopping`，由操作员处理。
- 终态以证据为准：进程退出码非零、final 中有错误或此前观察到 ERROR，显示 `failed`。退出码为零且最终快照与四份 CSV 齐全时，有 stop 请求为 `stopped`，运行到 deadline 为 `completed`；其余为 `interrupted`。
- 影子模型在 Nautilus 节点启动前被取消时没有 CSV：需要明确的 stop 请求、退出码 0、无错误的 final，且 final 记录 `stop_reason=stopped_before_model_ready`、`node_started=false`、`orders_enabled=false`，才显示 `stopped`；此时 `reportsAvailable` 为 false。
- 实例完成的前提是运行期间 data/exec 至少同时连接过一次且 `quote_count > 0`；否则写入 `final.error`，以失败结束。

## 验证

本地测试使用固定的 Python 假运行器和 UNIX socket，覆盖请求边界、清单校验、金额上限、单实例、
重复操作、PID 身份、生命周期与环境隔离。上线前在目标服务器上用已安装的 Nautilus 扩展做独立
Sandbox 冒烟：真实 WS、策略收到行情、L2 撮合、最终报表和 SIGTERM。

校验部署清单（不启动进程）：

```sh
/www/nautilus/operator/venv/bin/python /usr/local/lib/montlok-groups/server/group_runtime.py \
  --registry /etc/montlok-groups/registry.json --check-registry
```

安装 service 后执行 `systemctl daemon-reload` 与 `systemctl enable --now nautilus-group-supervisor.service`，
启动的是控制服务；策略实例由确认后的 `start` 操作启动。模板 `ExecStart` 用 operator venv 运行
轻量 supervisor；注册中的 `pythonPath` 指向装有 Nautilus 编译扩展、pandas、msgspec、psutil（模型
组另需 PyTorch 或 montlok）的引擎 Python。两套服务的配置和内存限制分别设置。

零订单连通冒烟：在 root 注册文件中临时发布独立的 `baseline-smoke` 组，使用同一固定来源，设置
`verifyOnly: true`、`maxDurationSeconds: 120` 和明确的虚拟预算，通过相同的 `prepare` 与确认流程
启动；`verifyOnly` 由 root 注册配置决定。验收：`status.json` 中 `instruments_seen > 0`、
`quote_count > 0`，data/exec 均已连接，`orders.csv`/`fills.csv` 为零订单；120 秒后生成 `final.json`
和全部 CSV。模拟撮合验证另发布一个 `verifyOnly: false` 的 Sandbox 组。

## 备份与恢复

备份 `registry.json` 及其引用的已审核配置和源代码、`runs.sqlite` 的一致快照以及实例输出。
supervisor 服务重启会结束其 cgroup 内的实例；恢复后通过状态查询确认旧实例为 interrupted、
completed 或 stopped，再按需启动新实例。

`deploy/backup.py` 把组材料写入 age 包中的 `group-runtime/`：发布清单及 settings/signals/strategy
引用、worker/server/hardened 源文件、数据库一致快照，以及实例的 request/manifest/status/final/view、
曲线、事件、CSV 和日志。`recovery.json` 记录文件哈希、原始路径、Python 依赖与可读取的安装版本，
venv 按清单重建。日志与曲线按备份时的长度流式读取；活动实例的最后一行可能不完整，元数据会注明。

模型恢复材料还包括 `state/models/releases.sqlite` 的一致快照、每个发布版本的权重、manifest、
来源和元数据，以及 root 配置选择的 guard、worker、预装 runner 与 settings。worker 已接受的
控制操作随其 SQLite 快照保存，副本中的 processing 改为 unknown。

恢复出的数据库副本清空 `pid/process_created`，并把活跃状态改为 `interrupted`；原数据库保持原样。
socket、pid 文件、SQLite WAL/SHM 和进程状态在新主机上重新生成；历史 manifest 中的 PID 是审计
记录。恢复发布版本后，模型实例由操作员启动。`check_backup.py` 校验组文件与模型文件哈希、发布
引用、活动版本指针、SQLite 一致性、运行依赖和恢复后的实例状态。

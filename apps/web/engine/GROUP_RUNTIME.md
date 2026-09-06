# 策略组运行服务

此版本将运行服务与 BFF 分开：BFF 重启不结束策略；`nautilus-group-supervisor.service` 拥有独立 cgroup、资源上限和状态目录。只支持**已发布的现货 CASH 1x Nautilus Sandbox**。它不会接管 `run-24h-02`，不会读取账户密钥，也不会执行上传区内的模型或代码。

## 当前执行含义

`group_worker.py` 复用已审核的 `SectorPaper` 冻结权重规则及 hardened engine 的风控、对账、告警、流式落盘和控制 socket。`marketTransport` 由 root 发布，缺省 `native_ws`；`public_rest_l2` 则直接复用已审核策略模块内的 `PublicOKXFactory` / `SnapshotOKXDataClient`，不新增适配器、不绕过 TLS 检查。撮合采用 L2 Sandbox 和单边 10 bps 手续费。manifest/status/final 和运行 API 都记录实际使用的传输方式；不会自动从 WS 降级到 REST。

该组仍是**固定信号的一次初始配置**，不声称运行实时机器学习或每分钟再平衡；日线研究的 5 个共同交易日重算尚未接入。新增 Alpha 必须先发布独立审核配置，不能因上传成功而获得启动能力。

## 部署目录

- `/usr/local/lib/montlok-groups/server/group_runtime.py`：独立服务及 BFF 客户端。
- `/usr/local/lib/montlok-groups/engine/group_worker.py`：固定启动器。
- `/usr/local/lib/montlok-groups/hardened/`：已验证 `b382994cde` 的 `alerts.py`、`commands.py`、`control.py`、`health.py`、`node_config.py`、`run.py`、`settings.py`。
- `/usr/local/lib/montlok-groups/releases/baseline_sector.py`：发布时冻结的 `sector_rotation_paper.py`，不要指向仍在研究编辑的文件。
- `/etc/montlok-groups/registry.json`、`baseline.settings.json`、`baseline.signals.json`：root 管理的审核清单。
- `/www/nautilus/group-runs/`：`nautilus` 可写；持久化 `runs.sqlite`、每个实例的请求、日志、控制 socket、状态、曲线和交易报表。

程序、Python 解释器、发布清单及其父目录必须由 root 拥有，且不可被 group/other 写入。解释器允许 root 管理的 venv 符号链接。BFF 只需能连接 `supervisor.sock`；不要给 BFF 提供 systemctl 或任意进程启动能力。

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

`baseline.settings.json` 使用 hardened `Settings` 格式：`environment=sandbox`、`instrument_types=["SPOT"]`、`strategies=[]`，品种与冻结信号严格一致，每个品种都配置正整数 `risk.max_notional_per_order`。保持告警 webhook、capture 和监控设置；不填写任何 OKX 密钥。运行器只覆盖本次 `state_dir`、`sandbox_balance`，并强制 capture。

信号文件必须是 `execution=nautilus_sandbox`、`account_type=CASH`、`leverage=1`，每个 `*-USDT.OKX` 的权重 0–10%，总权重不超过 100%。已发布文件内容或注册配置发生变化后，旧确认请求失效；新版本需重新加载 supervisor，已有实例的停止操作仍按实例身份进行。

## BFF 合约

### 已发布模型策略组

根注册文件可增加 `modelRuntime`。该配置只来自 root 管理的注册文件，HTTP
请求不能提供代码路径、设备覆盖、guard 或 runner 路径：

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
  "contractKey": {"rdt4quant_v1": "crypto:BTC-USDT"}
}
```

每个已发布不可变版本对应 `model-<manifestSha256 前 16 位>`。只有当前发布
指针所选版本能新启动；旧版本已有实例继续保留其原始权重、清单和命令行身份，
回滚不会替换运行中的模型。发布本身不创建实例。`mode` 初版仅允许 `shadow`。

GRU 固定选择 BTC-USDT；RDT 必须在 root 配置中显式选择已发布且已接通的
contract（当前 `crypto:BTC-USDT` 或 `token_hour:XNVDA`）。清单的输出头仍由
其不可变 `selectedHeadByDomain` 决定。root settings 必须包含目标执行品种的
正整数单笔金额上限，不从其他品种或账户预算猜测。

状态轮询只读取小发布索引和缓存的节点能力，不重复散列权重。预检和启动重新
验证 root 程序、guard、settings、主机预装 runner 以及发布清单/权重/来源文件
hash。PyTorch、NumPy、Nautilus 缺失或 CUDA/BF16 不满足时，启动能力为 false。
本机 capability 检查不是模型 warmup；worker 报告真实 warmup 完成及行情/执行
连接后才显示 running。模型进程和 guard 都在目标主机本地运行。

节点能力还必须包含已安装的显式 Python 原生数据桥，旧版扩展即使 PyTorch
可用也不能启动模型。running 进一步要求 worker 的 `marketReady=true`：真实
原生行情已投递、quote 未过期、模型输入与目标有效，不以单纯连接成功替代。
`maxConcurrentRuns` 默认 2、允许 1–16，统计全部仍存活的模型版本实例（包括
启动中和停止中），独立于每版本单实例限制。槽位满时节点 ready 可保持 true，
但新启动能力为 false，并显示使用数量；原有基线组不占用这个模型槽位。

`GroupRuntimeClient(socket_path)` 提供四个 async 方法：

- `status(group_id=None)`：`{available, groups:[{groupId, name, mode, environment, ready, runId, supportedActions, capabilities, defaultBudgetUsdt, maxBudgetUsdt, maxDurationSeconds, runs}]}`。
- `prepare({groupId, action, budgetUsdt?, durationSeconds?, runId?})`：返回 `{request, mode, groupName, effect, signalVersion, strategyVersion}`。`request` 是需要绑定确认票据的规范化请求，含 `registryVersion`。
- `execute(request, operation_id)`：返回真实启动/控制结果。同一 `operation_id` 的相同请求不重复执行；不同请求复用编号被拒绝；中断后未知结果返回 `unknown`，不会再次启动。
- `receipt(operation_id)`：仅查询该操作的持久回执。Supervisor 到 worker 的 `halt/reduce/resume` 使用同一个编号；超时与断链返回 `receiptStatus=unknown`，不能当作失败或自动重发。Worker 在执行前后分别落盘 SQLite 日志，Supervisor 重启后仍可查询；worker 已停止时只读其已存操作日志。迟到回执不覆盖后来发生的运行状态。

BFF 提供 `GET /api/operations/{id}` 只读查询入口。已记录稳定 operator 归属的新回执支持同一用户重登后读取；旧无 owner 的回执保留原 session 限制。`POST /api/execute` 重放仍限定原会话。操作回执完成不等于运行结束：`stop` 持久化 stopRequested 后发送 SIGTERM，回执可为 completed/stopping，最终 stopped 仍需退出与报告证据。关闭控制服务先拒绝新写请求并等待已接受操作完成，再停止引擎。

动作仅 `start | halt | reduce | resume | stop`。启动需明确虚拟资金和最长时长；其他动作必须带 supervisor 自己创建的 `runId`。BFF 在 `prepare/execute` 前验证管理员、会话、CSRF 和一次性确认票据。`status.runs[].runDir` 供服务端读取图表，向浏览器输出前可移除绝对路径。

状态语义：`starting` 是子进程已创建、未证实行情就绪；仅同时收到匹配 PID 的 sandbox 控制响应及 data/exec 连接状态才显示 `running`。`stop` 先返回 `stopping`，干净退出才成为 `stopped`。超时未退出不会冒称停止，也不自动升级为强制杀死。

`halt` 暂停并撤销该实例挂单；`reduce` 仅允许减仓；`resume` 恢复该模拟实例。`stop` 结束整个模拟进程并输出报表，不代表平掉真实账户仓位。旧进程、其他策略组进程、PID 复用和不匹配命令行均不接受信号。

## 验证与恢复

本地测试使用固定 Python 假运行器和 UNIX socket，不连接交易所，覆盖请求边界、清单校验、金额上限、单实例、重复操作、PID 身份、生命周期与环境隔离。上线前仍需要使用目标服务器安装的 Nautilus 扩展做独立 Sandbox 冒烟，验证真实 WS、策略收到行情、L2 撮合、最终报表和 SIGTERM。

仅验证部署清单，不启动进程：

```sh
/www/nautilus/operator/venv/bin/python /usr/local/lib/montlok-groups/server/group_runtime.py \
  --registry /etc/montlok-groups/registry.json --check-registry
```

安装独立 service 后执行 `systemctl daemon-reload` 与 `systemctl enable --now nautilus-group-supervisor.service` **只会启动控制服务，不会运行任何策略**。模板 `ExecStart` 使用已有 operator venv 来运行轻量 supervisor；注册中的 `pythonPath` 必须指向装有当前 Nautilus 编译扩展、pandas、msgspec、psutil 的引擎 Python，不能把两者混淆。保持两套服务的配置和内存限制独立。

零订单原生连通冒烟：在 root 注册文件中临时发布独立 `baseline-smoke` 组，使用同一固定来源，设置 `verifyOnly: true`、`maxDurationSeconds: 120`、明确虚拟预算。通过相同 `prepare` 和确认流程启动该组。`verifyOnly` 只接受 root 注册配置，浏览器不能将其从 true 改成 false。验收需见该实例 `status.json` 的 `instruments_seen > 0`、`quote_count > 0`，控制状态 data/exec 连接均正常，`orders.csv`/`fills.csv` 为零订单；120 秒后生成 `final.json` 和全部 CSV。后续模拟撮合验证可发布另一个明确非 `verifyOnly` 的 Sandbox 实例，但不能更改既有策略运行或把该步骤称为实盘测试。

备份 `registry.json`、其引用的已审核配置和源代码、`runs.sqlite` 的一致性 SQLite 快照及实例输出。BFF 重启不影响独立服务。Supervisor 服务重启会结束其自身 cgroup 内的模拟实例；不会自动重新开仓。恢复后通过状态查询判断旧实例的 `interrupted/completed/stopped`，重新启动必须建立新实例。

`deploy/backup.py` 现在把新组材料写入 age 包中的 `group-runtime/`：发布清单及 settings/signals/strategy 引用、worker/server/hardened 源文件、数据库一致快照，以及实例 request/manifest/status/final/view、曲线、事件、CSV、日志。附 `recovery.json` 文件哈希清单、原始路径与 Python 环境依赖/可读取的安装版本，不打包 venv。日志与曲线按备份时观察到的长度流式读取；活动实例的尾行可能未完成，元数据明确记录这一点。

模型恢复材料另包括 `state/models/releases.sqlite` 的一致快照、每个发布版本的
权重、manifest、来源和全部元数据、root 配置选择的 guard/worker/预装 runner
及 settings。`incoming`、socket、SQLite WAL/SHM 不作为恢复文件；已接受 worker
控制操作通过其 SQLite 备份快照保存，副本里的 processing 改为 unknown，原日志
保持不变。`check_backup.py` 校验活动版本指针、全部模型文件哈希和运行依赖；
恢复发布版本不代表启动模型，恢复数据库也不重放操作。

恢复数据库副本清空 `pid/process_created`，活跃状态改为 `interrupted`，**不改原数据库**。socket、pid 文件、SQLite WAL/SHM 与进程执行态均不恢复。历史 manifest 里的 PID 仅是审计证据；恢复不会启动任何策略。`check_backup.py` 验证全部组文件哈希、发布引用、一致 SQLite 和不可直接恢复运行的状态。

终态以证据为准：进程退出码非零、final 中存在错误，或此前观察到 ERROR，均显示 `failed`。退出码为零且最终快照及四份 CSV 完整时，有明确 stop 请求才显示 `stopped`，自然运行到 deadline 才显示 `completed`；其余情况为 `interrupted`。

唯一不要求四份 CSV 的停止特例是尚未启动 Nautilus 节点的影子模型取消：必须
已有明确 stop 请求、进程退出码为 0、有效 final 无错误，且 final 同时记录
`stop_reason=stopped_before_model_ready`、`node_started=false`、`orders_enabled=false`。
该情况仅显示 stopped，不显示 completed，`reportsAvailable` 仍为 false；普通
运行或缺少任一证据时保留原有规则，不伪造账户/成交报告。

运行期间从未观察到 data/exec 同时连接成功，或 `quote_count=0`，都会写入 `final.error` 并以异常结束，即使已经到达计划时间。零订单连通验证也必须真的收到行情，不能用“空跑到截止时间”代替成功。

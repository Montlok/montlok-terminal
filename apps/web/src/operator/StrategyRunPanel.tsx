import { Link, useModel } from '@umijs/max';
import { Alert, Button, InputNumber, Select, Tooltip } from 'antd';
import { useCallback, useEffect, useRef, useState } from 'react';
import {
  executionModeLabel,
  strategyDisplayName,
} from '../pages/StrategyGroups/groupModel';
import { api, number, type Row, timeOf } from './api';
import { ConfirmOperation } from './ConfirmOperation';
import { ModelRuntime } from './ModelRuntime';
import { OperationProgress } from './OperationProgress';
import { usePoll } from './usePoll';
import './strategyRun.css';

const ACTIONS = {
  start: '开始运行',
  halt: '暂停开仓',
  reduce: '仅减仓',
  resume: '恢复运行',
  stop: '停止运行',
  cancel: '撤单并暂停',
  flatten: '平仓',
} as const;
const STATES: Record<string, string> = {
  starting: '启动中',
  running: '运行中',
  halted: '已暂停',
  reducing: '仅减仓',
  stopping: '停止中',
  stopped: '已停止',
  completed: '已结束',
  failed: '运行失败',
  unresponsive: '引擎未响应',
  unknown: '结果待核对',
  error: '引擎异常',
  recovering: '恢复中',
  engine_stopped: '进程收尾中',
  interrupted: '运行中断',
};

export function StrategyRunPanel({ instrument }: { instrument?: string }) {
  const {
    selectedGroup,
    setSelectedGroup,
    selectedRuns,
    setSelectedRuns,
    profiles = [],
  } = useModel('operator', (model) => ({
    selectedGroup: model.selectedGroup,
    setSelectedGroup: model.setSelectedGroup,
    selectedRuns: model.selectedRuns,
    setSelectedRuns: model.setSelectedRuns,
    profiles: model.profiles,
  }));
  const selectedRun = selectedRuns?.[selectedGroup] || '';
  const actionScope = `${selectedGroup}/${selectedRun}`;
  const currentActionScope = useRef(actionScope);
  currentActionScope.current = actionScope;
  const { initialState } = useModel('@@initialState');
  const admin = initialState?.currentUser?.access === 'admin';
  const [groups, setGroups] = useState<Row[]>([]);
  const [runtime, setRuntime] = useState<Row>();
  const [budget, setBudget] = useState<number | null>(null);
  const [minutes, setMinutes] = useState<number | null>(60);
  const [runMode, setRunMode] = useState<'continuous' | 'timed' | null>(null);
  const [executionSettings, setExecutionSettings] = useState<Row>();
  const [checked, setChecked] = useState<Row>();
  const [ticket, setTicket] = useState<Row>();
  const [busy, setBusy] = useState(false);
  const [busyAction, setBusyAction] = useState('');
  const [error, setError] = useState('');
  const [receipt, setReceipt] = useState<Row>();
  const [reductionScope, setReductionScope] = useState<string[]>([]);
  const current = useRef(selectedGroup);
  current.current = selectedGroup;

  const loadGroups = useCallback(async (signal: AbortSignal) => {
    try {
      const result = await api('strategy-groups');
      if (!signal.aborted) setGroups(result.groups || []);
    } catch (reason) {
      if (!signal.aborted) setError(String(reason));
    }
  }, []);
  usePoll(loadGroups, 15000);
  const loadRuntime = useCallback(
    async (signal?: AbortSignal) => {
      try {
        const result = await api(
          `strategy-groups/${encodeURIComponent(selectedGroup)}/runtime`,
        );
        if (!signal?.aborted && current.current === selectedGroup)
          setRuntime(result);
      } catch (reason) {
        if (!signal?.aborted && current.current === selectedGroup)
          setRuntime({ available: false, reason: String(reason) });
      }
    },
    [selectedGroup],
  );
  usePoll(loadRuntime, 3000);
  useEffect(() => {
    setRuntime(undefined);
    setBudget(null);
    setRunMode(null);
    setExecutionSettings(undefined);
    setChecked(undefined);
    setTicket(undefined);
    setReceipt(undefined);
    setError('');
    setReductionScope([]);
  }, [selectedGroup]);

  const group = groups.find((item) => item.id === selectedGroup);
  const control = runtime?.groups?.find(
    (item: Row) => item.groupId === selectedGroup,
  );
  const run = control?.runs?.find(
    (item: Row) =>
      item.runId ===
      (selectedRun || (group?.managed ? group.runId : undefined)),
  );
  const legacy =
    !selectedRun && group?.runId && !group.managed ? group : undefined;
  const observedRun = run || legacy;
  const capability = control?.capabilities || {};
  const live = control?.kind === 'live';
  const pendingStart = control?.pendingStart;
  useEffect(() => {
    if (pendingStart?.operationId)
      setReceipt((previous) =>
        previous?.id === pendingStart.operationId
          ? previous
          : {
              id: pendingStart.operationId,
              action: 'start',
              status: 'processing',
              result: { groupId: selectedGroup, receiptStatus: 'processing' },
            },
      );
  }, [pendingStart?.operationId, selectedGroup]);
  useEffect(() => {
    if (
      receipt?.action === 'start' &&
      receipt.result?.runId &&
      receipt.result?.groupId === selectedGroup
    )
      setSelectedRuns((previous) =>
        previous[selectedGroup] === receipt.result.runId
          ? previous
          : { ...previous, [selectedGroup]: receipt.result.runId },
      );
  }, [
    receipt?.action,
    receipt?.result?.runId,
    receipt?.result?.groupId,
    selectedGroup,
    setSelectedRuns,
  ]);
  const continuousAvailable =
    live && control?.durationPolicy?.continuous === true;
  const continuous =
    continuousAvailable && (runMode ?? 'continuous') === 'continuous';
  const durationSeconds = continuous ? 0 : (minutes || 0) * 60;
  const durationLabel = continuous
    ? '持续运行 · 手动停止'
    : `${minutes || 0} 分钟`;
  const effectiveExecutionSettings =
    executionSettings ?? control?.executionOptions?.defaults;
  const effectiveBudget = budget ?? Number(control?.defaultBudgetUsdt || 2000);
  const inputs = live
    ? {
        durationSeconds,
        ...(effectiveExecutionSettings
          ? { executionSettings: effectiveExecutionSettings }
          : {}),
      }
    : { budgetUsdt: effectiveBudget, durationSeconds };
  const fingerprint = JSON.stringify([actionScope, inputs]);
  const latestFingerprint = useRef(fingerprint);
  latestFingerprint.current = fingerprint;
  useEffect(() => {
    setTicket((previous) =>
      previous?.operation?.name === 'start' ? undefined : previous,
    );
  }, [fingerprint]);
  const validCheck =
    checked?.fingerprint === fingerprint && checked?.ok !== false;
  const boundProfile = profiles.find(
    (profile: Row) => profile.id === control?.profileId,
  );
  const executionLabel = control
    ? live
      ? 'OKX 实盘'
      : '研究运行'
    : '读取执行配置';
  const activeRun = control?.runs?.find(
    (item: Row) => item.runId === control?.runId,
  );
  const model = observedRun?.engine?.model ?? observedRun?.model;
  const runInstruments = (observedRun?.engine?.inventory?.pairs || []).map(
    (item: Row) => item.instrument as string,
  );
  const flattenStatus = observedRun?.engine?.flatten;
  const eligible = admin && !!control && runtime?.available && !busy;
  const controllable = eligible && run && run.runId === control?.runId;
  const canStopLegacy = admin && !busy && legacy?.status === 'running';
  const eligibilityReason = !admin
    ? '需要操作员权限'
    : busy
      ? '上一项请求正在处理'
      : !runtime
        ? '正在读取运行服务状态'
        : !runtime.available
          ? runtime.reason || '运行服务正在重新连接'
          : !control
            ? '该策略组未配置运行服务'
            : '';
  const newRunReason =
    eligibilityReason ||
    (receipt?.action === 'start' &&
    ['processing', 'unknown'].includes(
      receipt?.result?.receiptStatus || receipt?.status,
    )
      ? '启动请求正在处理'
      : '') ||
    (control?.nodeCompatibility?.ready === false
      ? control.nodeCompatibility.reason || '当前节点缺少模型需要的运行资源'
      : '') ||
    (!capability.start
      ? capability.reason || '等待当前运行结束或更新策略配置'
      : '');
  const runReason =
    eligibilityReason ||
    (!run
      ? '选择一个运行实例'
      : run.runId !== control?.runId
        ? '此实例已结束或不是当前活动实例'
        : '');
  const actionReasons = {
    halt:
      runReason ||
      (!capability.halt
        ? run?.status === 'halted'
          ? '此实例已暂停开仓'
          : '当前状态无需暂停开仓'
        : ''),
    reduce:
      runReason ||
      (!capability.reduce
        ? run?.status === 'reducing'
          ? '此实例已处于仅减仓状态'
          : '当前状态无需切换为仅减仓'
        : ''),
    resume:
      runReason ||
      (!capability.resume
        ? capability.resumeReason || '仅已暂停或仅减仓的实例可恢复'
        : ''),
    cancel: runReason || (!capability.cancel ? '当前运行未提供撤单控制' : ''),
    flatten:
      runReason ||
      (!capability.flatten ? '当前运行未提供平仓控制或正在处理平仓' : ''),
    stop: canStopLegacy
      ? ''
      : runReason || (!capability.stop ? '当前状态没有可停止的运行进程' : ''),
  };
  const startLabel = !control
    ? '启动策略'
    : live &&
        observedRun &&
        ['failed', 'error', 'interrupted'].includes(observedRun.status)
      ? '重新启动实盘策略'
      : live
        ? '启动实盘策略'
        : '启动研究运行';
  const receiptAction =
    ACTIONS[receipt?.action as keyof typeof ACTIONS] || '操作';
  const receiptUnknown = ['unknown', 'processing'].includes(
    receipt?.result?.receiptStatus || receipt?.status,
  );
  const receiptFailed =
    !receiptUnknown &&
    (receipt?.status === 'error' ||
      receipt?.status === 'failed' ||
      receipt?.result?.status === 'failed' ||
      receipt?.result?.receiptStatus === 'failed' ||
      !!receipt?.result?.error);
  const receiptRun =
    control?.runs?.find((item: Row) => item.runId === receipt?.result?.runId) ||
    (legacy?.runId === receipt?.result?.runId ? legacy : undefined);
  const receiptTitle = receiptUnknown
    ? `${receiptAction} · 正在核对结果`
    : receiptFailed
      ? `${receiptAction}失败`
      : receipt?.action === 'stop'
        ? `停止请求已受理 · ${STATES[receiptRun?.status || receipt?.result?.status] || '等待运行状态确认'}`
        : `${receiptAction}已受理 · ${STATES[receiptRun?.status || receipt?.result?.status] || '等待运行状态确认'}`;
  const receiptId = receipt?.id;
  const refreshReceipt = useCallback(
    async (signal: AbortSignal) => {
      if (!receiptId) return;
      try {
        const result = await api(`operations/${encodeURIComponent(receiptId)}`);
        if (!signal.aborted)
          setReceipt((previous) =>
            previous?.id === receiptId ? { ...previous, ...result } : previous,
          );
      } catch {
        // An unavailable receipt is not permission to replay its confirmed action.
      }
    },
    [receiptId],
  );
  usePoll(refreshReceipt, 2000, !!receiptId && receiptUnknown);
  useEffect(() => {
    setTicket(undefined);
    setReceipt((previous) =>
      previous?.result?.runId && previous.result.runId !== selectedRun
        ? undefined
        : previous,
    );
  }, [selectedRun]);

  async function preflight() {
    setBusy(true);
    setBusyAction('读取账户和交易范围');
    setError('');
    setChecked(undefined);
    const checkingScope = actionScope;
    try {
      const result = await api(
        `strategy-groups/${encodeURIComponent(selectedGroup)}/preflight`,
        inputs,
      );
      if (currentActionScope.current === checkingScope)
        setChecked({ ...result, fingerprint });
    } catch (reason) {
      if (currentActionScope.current === checkingScope)
        setError(String(reason));
    } finally {
      setBusy(false);
    }
  }

  async function begin() {
    // The server's prepare endpoint validates configuration and captures current
    // inventory. Do not duplicate that slow read with a second preflight call.
    await prepare('start');
  }

  async function prepare(action: keyof typeof ACTIONS) {
    setBusy(true);
    setBusyAction(
      action === 'start' ? '准备启动确认单' : `准备${ACTIONS[action]}`,
    );
    setError('');
    const preparingScope = actionScope;
    const preparingFingerprint = fingerprint;
    try {
      const result = await api(
        'prepare',
        action === 'stop' && canStopLegacy
          ? {
              kind: 'native',
              name: 'stop',
              arguments: { runId: legacy.runId },
            }
          : {
              kind: 'group',
              name: action,
              arguments: {
                groupId: selectedGroup,
                ...(action === 'start' ? inputs : { runId: run?.runId }),
                ...(action === 'flatten'
                  ? {
                      instruments: reductionScope.length
                        ? reductionScope
                        : runInstruments,
                    }
                  : {}),
              },
            },
      );
      if (
        currentActionScope.current === preparingScope &&
        (action !== 'start' ||
          latestFingerprint.current === preparingFingerprint)
      )
        setTicket({ ...result, actionScope: preparingScope });
    } catch (reason) {
      if (currentActionScope.current === preparingScope)
        setError(String(reason));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className={`strategy-run-panel ${activeRun ? '' : 'run-setup'}`}>
      {busy && <OperationProgress label={busyAction} />}
      {!busy &&
        (pendingStart || (receiptUnknown && receipt?.action === 'start')) && (
          <OperationProgress
            label="启动请求已受理，正在准备运行"
            startedAt={pendingStart?.at ? pendingStart.at * 1000 : undefined}
          />
        )}
      <section
        className={`run-execution-context ${live ? 'run-context-live' : ''}`}
        aria-label="策略执行环境"
      >
        <strong>{executionLabel}</strong>
        <span>
          {!control
            ? '正在读取策略配置'
            : live
              ? `执行账户：${boundProfile?.name || control?.profileId || '读取中'}`
              : '研究参数与信号'}
        </span>
        {activeRun && (
          <span>当前运行：{STATES[activeRun.status] || activeRun.status}</span>
        )}
        {activeRun?.engine && (
          <section className="run-live-summary" aria-label="运行执行概况">
            <span>
              {activeRun.engine.allocation?.phase === 'ALLOCATED'
                ? '初始调仓完成 · 持仓跟踪'
                : activeRun.status === 'starting'
                  ? '连接行情与交易接口'
                  : activeRun.engine.allocation?.phase
                    ? '执行调仓'
                    : '跟踪运行'}
            </span>
            <span>
              已接受 {activeRun.engine.ordersAccepted ?? '—'} · 成交{' '}
              {activeRun.engine.fillsTotal ?? '—'} · 拒单{' '}
              {activeRun.engine.ordersRejected ?? '—'}
            </span>
            {activeRun.engine.execution && (
              <span>
                近 60 秒：报单{' '}
                {activeRun.engine.execution.lastMinute?.submit ?? 0}
                {' · '}撤单 {activeRun.engine.execution.lastMinute?.cancel ?? 0}
                {' · '}成交 {activeRun.engine.execution.lastMinute?.fill ?? 0}
                {' · '}工作委托 {activeRun.engine.execution.openOrders ?? 0}
              </span>
            )}
          </section>
        )}
        {activeRun && observedRun?.runId !== activeRun.runId && (
          <Button
            size="small"
            onClick={() =>
              setSelectedRuns((previous) => ({
                ...previous,
                [selectedGroup]: activeRun.runId,
              }))
            }
          >
            查看当前运行
          </Button>
        )}
      </section>
      <label className="run-field" htmlFor="run-group">
        <span>策略组</span>
        <Select
          id="run-group"
          aria-label="运行策略组"
          value={selectedGroup}
          onChange={setSelectedGroup}
          showSearch={{ optionFilterProp: 'label' }}
          options={groups.map((item) => ({
            value: item.id,
            label: strategyDisplayName(item),
            title: executionModeLabel(item),
          }))}
        />
      </label>
      <label className="run-field" htmlFor="inspector-run">
        <span>运行记录</span>
        <Select
          id="inspector-run"
          aria-label="控制运行实例"
          value={selectedRun}
          onChange={(value) =>
            setSelectedRuns((previous) => ({
              ...previous,
              [selectedGroup]: value,
            }))
          }
          options={[
            {
              value: '',
              label: group?.runId
                ? `${group.managed ? '当前 / 最近一次' : '历史'} · ${group.runId}`
                : control?.runs?.length
                  ? `选择记录 · 共 ${control.runs.length} 个实例`
                  : '等待首次运行',
            },
            ...(control?.runs || []).map((item: Row) => ({
              value: item.runId,
              label: `${item.runId} · ${STATES[item.status] || item.status}`,
            })),
          ]}
        />
      </label>
      {observedRun && (
        <section className="run-section" aria-label="当前运行状态与资金范围">
          <h3>{run?.runId === control?.runId ? '当前运行' : '运行记录详情'}</h3>
          {['failed', 'error', 'interrupted', 'unresponsive'].includes(
            observedRun.status,
          ) && (
            <Alert
              type="error"
              showIcon
              title="策略运行已停止"
              description={
                observedRun.error ||
                observedRun.engine?.lastOrderError ||
                '请查看引擎健康和最后一次运行记录'
              }
            />
          )}
          <dl className="run-facts">
            <div>
              <dt>执行环境</dt>
              <dd>{control?.mode === 'live' ? 'OKX 实盘' : '研究运行'}</dd>
            </div>
            <div>
              <dt>运行状态</dt>
              <dd>
                {observedRun
                  ? STATES[observedRun.status] || observedRun.status
                  : control?.runs?.length
                    ? `选择实例 · 共 ${control.runs.length} 个`
                    : '等待首次运行'}
              </dd>
            </div>
            <div>
              <dt>模型 / 信号</dt>
              <dd>
                {control?.name
                  ? strategyDisplayName(control)
                  : group?.description || '—'}
              </dd>
            </div>
            {observedRun && (
              <div>
                <dt>运行编号</dt>
                <dd title={observedRun.runId}>{observedRun.runId}</dd>
              </div>
            )}
            {observedRun && (
              <div>
                <dt>启动时间</dt>
                <dd>{timeOf(observedRun.startedAt)}</dd>
              </div>
            )}
            <div>
              <dt>实例初始预算</dt>
              <dd>
                {live
                  ? control?.exposureCapUsdt
                    ? `新增净敞口 ≤ ${number(control.exposureCapUsdt)} USDT`
                    : '账户库存'
                  : `${number(observedRun.budgetUsdt ?? observedRun.metrics?.capital)} USDT`}
              </dd>
            </div>
            <div>
              <dt>实例计划时长</dt>
              <dd>
                {observedRun?.durationSeconds === 0 && live
                  ? '持续运行 · 手动停止'
                  : observedRun?.durationSeconds
                    ? `${number(observedRun.durationSeconds / 60)} 分钟`
                    : '未记录'}
              </dd>
            </div>
            <div>
              <dt>资金使用</dt>
              <dd>
                {live
                  ? observedRun?.engine?.execution?.reservedCashUsdt != null
                    ? `${number(observedRun.engine.execution.reservedCashUsdt)} USDT 已预留`
                    : observedRun?.engine?.execution?.openOrders != null
                      ? `${observedRun.engine.execution.openOrders} 个工作委托`
                      : '随账户库存实时计算'
                  : observedRun?.budgetWindow?.usedUsdt === undefined
                    ? '运行记录同步中'
                    : `${number(observedRun.budgetWindow.usedUsdt)} USDT`}
              </dd>
            </div>
          </dl>
          {observedRun?.status === 'halted' && (
            <p className="run-detail">已暂停开仓，可按需恢复运行。</p>
          )}
          <div className="run-actions run-control-actions">
            {(['halt', 'reduce', 'resume', 'stop', 'cancel'] as const)
              .filter((action) => live || action !== 'cancel')
              .map((action) => (
                <div className="run-action" key={action}>
                  <Tooltip title={actionReasons[action]}>
                    <span>
                      <Button
                        block
                        danger={action === 'stop'}
                        aria-describedby={
                          actionReasons[action]
                            ? `run-${action}-reason`
                            : undefined
                        }
                        disabled={
                          action === 'stop'
                            ? !canStopLegacy &&
                              (!controllable || !capability.stop)
                            : !controllable || !capability[action]
                        }
                        onClick={() => void prepare(action)}
                      >
                        {ACTIONS[action]}
                      </Button>
                    </span>
                  </Tooltip>
                  {actionReasons[action] && (
                    <small className="run-sr-only" id={`run-${action}-reason`}>
                      {actionReasons[action]}
                    </small>
                  )}
                </div>
              ))}
          </div>
          {live && (
            <div className="run-reduction">
              <label className="run-field" htmlFor="run-flatten-instruments">
                <span>平仓范围</span>
                <Select
                  id="run-flatten-instruments"
                  mode="multiple"
                  aria-label="平仓品种"
                  placeholder="本组全部品种"
                  value={reductionScope}
                  disabled={!controllable || !capability.flatten}
                  onChange={(values) => {
                    setReductionScope(values);
                    setTicket(undefined);
                  }}
                  options={runInstruments.map((value: string) => ({
                    value,
                    label: value.replace('-', '/'),
                  }))}
                />
              </label>
              <Tooltip title={actionReasons.flatten}>
                <span>
                  <Button
                    block
                    danger
                    disabled={
                      !controllable ||
                      !capability.flatten ||
                      !runInstruments.length
                    }
                    onClick={() => void prepare('flatten')}
                  >
                    平仓
                  </Button>
                </span>
              </Tooltip>
              {flattenStatus && (
                <div role="status" className="run-detail">
                  <strong>
                    平仓进度 ·{' '}
                    {(
                      {
                        cancelling: '撤单中',
                        reducing: '执行中',
                        completed: '已完成',
                        incomplete: '剩余待处理',
                      } as Row
                    )[flattenStatus.phase] || flattenStatus.phase}
                  </strong>
                  {Object.entries(flattenStatus.results || {}).map(
                    ([symbol, value]) => {
                      const row = value as Row;
                      return (
                        <div key={symbol}>
                          {symbol} ·{' '}
                          {(
                            {
                              flat: '已平',
                              dust: '余量低于最小数量',
                              submitted: '已报单',
                              waiting_quote: '等待报价',
                              waiting_balance: '等待余额',
                              balance_or_minimum: '可用数量不足',
                              attempt_limit: '剩余未成交',
                            } as Row
                          )[row.state] || row.state}{' '}
                          · 剩余 {row.remaining}
                        </div>
                      );
                    },
                  )}
                </div>
              )}
            </div>
          )}
        </section>
      )}
      <section className="run-section run-launch" aria-label="新建独立运行实例">
        <h3>启动策略</h3>
        {control?.executionPolicy && (
          <section className="run-detail" aria-label="策略执行方式">
            <strong>{control.executionPolicy.label}</strong>
            <p>{control.executionPolicy.description}</p>
          </section>
        )}
        {control?.executionOptions && effectiveExecutionSettings && (
          <section className="run-execution-settings" aria-label="执行参数">
            <label className="run-field" htmlFor="execution-mode">
              <span>执行方式</span>
              <Select
                id="execution-mode"
                aria-label="执行方式"
                value={effectiveExecutionSettings.mode}
                disabled={!admin || busy}
                options={control.executionOptions.modes}
                onChange={(value) =>
                  setExecutionSettings({
                    ...effectiveExecutionSettings,
                    mode: value,
                  })
                }
              />
            </label>
            {(
              [
                ['quoteIntervalMs', '决策间隔 / ms'],
                ['requoteThresholdBps', '改价阈值 / bps'],
                ['quoteSizeLots', '单笔数量 / 最小手数'],
                ['inventoryBandLots', '库存带 / 最小手数'],
                ['orderExpirySecs', '委托更新 / 秒'],
                ['maxOrdersPerSecond', '报单上限 / 秒'],
                ['minimumEdgeBps', '最低边际 / bps'],
                ['maxOrderNotionalUsdt', '单笔金额上限 / USDT'],
              ] as const
            ).map(([key, label]) => {
              const bounds = control.executionOptions.bounds?.[key] || [];
              return (
                <label
                  className="run-field"
                  htmlFor={`execution-${key}`}
                  key={key}
                >
                  <span>{label}</span>
                  <InputNumber
                    id={`execution-${key}`}
                    aria-label={label}
                    value={effectiveExecutionSettings[key]}
                    min={bounds[0]}
                    max={bounds[1]}
                    precision={0}
                    disabled={!admin || busy}
                    onChange={(value) =>
                      setExecutionSettings({
                        ...effectiveExecutionSettings,
                        [key]: value,
                      })
                    }
                  />
                </label>
              );
            })}
          </section>
        )}
        <p className="run-detail">
          {!control
            ? '读取资金范围'
            : live
              ? control?.exposureCapUsdt
                ? `新增净敞口上限 ${number(control.exposureCapUsdt)} USDT`
                : '资金范围：执行账户的可用资产'
              : '资金范围：本次研究运行预算'}
        </p>
        <div className="run-inputs">
          {control && !live && (
            <label className="run-field" htmlFor="run-budget">
              <span>运行预算 / USDT</span>
              <InputNumber
                id="run-budget"
                aria-label="运行预算 USDT"
                value={effectiveBudget}
                min={0.01}
                max={
                  control?.maxBudgetUsdt == null
                    ? undefined
                    : Number(control.maxBudgetUsdt)
                }
                precision={2}
                disabled={!admin || busy}
                onChange={setBudget}
              />
            </label>
          )}
          {continuousAvailable && (
            <label className="run-field" htmlFor="run-mode">
              <span>运行方式</span>
              <Select
                id="run-mode"
                aria-label="运行方式"
                value={continuous ? 'continuous' : 'timed'}
                disabled={!admin || busy}
                onChange={setRunMode}
                options={[
                  { value: 'continuous', label: '持续运行 · 手动停止' },
                  { value: 'timed', label: '定时结束' },
                ]}
              />
            </label>
          )}
          {!continuous && (
            <label className="run-field" htmlFor="run-minutes">
              <span>运行时长 / 分钟</span>
              <InputNumber
                id="run-minutes"
                aria-label="运行时长 分钟"
                value={minutes}
                min={1}
                precision={0}
                max={
                  control?.maxDurationSeconds == null
                    ? undefined
                    : Math.floor(control.maxDurationSeconds / 60)
                }
                disabled={!admin || busy}
                onChange={setMinutes}
              />
            </label>
          )}
        </div>
        <div className="run-actions">
          <Button
            block
            disabled={!!newRunReason}
            aria-describedby={newRunReason ? 'run-action-reason' : undefined}
            loading={busy}
            onClick={() => void preflight()}
          >
            检查配置
          </Button>
          <Button
            block
            type="primary"
            aria-label="开始运行"
            disabled={!!newRunReason}
            loading={busy}
            aria-describedby={newRunReason ? 'run-action-reason' : undefined}
            onClick={() => void begin()}
          >
            {startLabel}
          </Button>
          {newRunReason && <small id="run-action-reason">{newRunReason}</small>}
        </div>
        {validCheck && (
          <p className="run-check-result">
            {live
              ? `${control?.exposureCapUsdt ? `新增净敞口 ≤ ${number(control.exposureCapUsdt)} USDT · ` : ''}${checked?.liveInventory?.pairs?.length || 0} 个交易对 · ${durationLabel}`
              : `配置已核验 · ${number(effectiveBudget)} USDT · ${durationLabel}`}
          </p>
        )}
      </section>
      {(runtime?.reason || capability.reason) &&
        (runtime?.reason || capability.reason) !== newRunReason && (
          <p className="run-detail">{runtime?.reason || capability.reason}</p>
        )}
      {error && <Alert type="error" title={error} />}
      {receipt && (
        <Alert
          type={
            receiptFailed || receiptRun?.status === 'failed' ? 'error' : 'info'
          }
          title={receiptTitle}
          description={
            (receiptUnknown ? '正在核对操作结果。' : receipt.result?.error) ||
            receiptRun?.error ||
            (receipt.action === 'stop' &&
            !['stopped', 'completed', 'failed', 'interrupted'].includes(
              receiptRun?.status,
            )
              ? '正在等待引擎完成停止。'
              : undefined)
          }
        />
      )}
      <div className="run-links">
        <Link to="/strategies/groups/overview">运行与收益</Link>
        <Link to="/strategies/resources/models">模型发布</Link>
      </div>
      {model && (
        <section className="run-section">
          <h3>模型运行</h3>
          <ModelRuntime model={model} />
        </section>
      )}
      {instrument && (
        <p className="run-detail">
          行情观察：{instrument} · 下单品种以策略配置为准
        </p>
      )}
      <ConfirmOperation
        ticket={ticket}
        onClose={() => setTicket(undefined)}
        onComplete={(result) => {
          if (ticket?.actionScope !== currentActionScope.current) return;
          if (result.result?.runId && ticket?.operation?.kind === 'group')
            setSelectedRuns((previous) => ({
              ...previous,
              [selectedGroup]: result.result.runId,
            }));
          setReceipt({ ...result, action: ticket?.operation?.name });
          setChecked(undefined);
          void loadRuntime();
        }}
      />
    </div>
  );
}

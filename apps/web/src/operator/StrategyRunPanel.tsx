import { Link, useModel } from '@umijs/max';
import { Alert, Button, InputNumber, Select } from 'antd';
import { useCallback, useEffect, useRef, useState } from 'react';
import { api, number, type Row, timeOf } from './api';
import { ConfirmOperation } from './ConfirmOperation';
import { ModelRuntime } from './ModelRuntime';
import { usePoll } from './usePoll';
import './strategyRun.css';

const ACTIONS = {
  start: '开始运行',
  halt: '暂停开仓',
  reduce: '仅减仓',
  resume: '恢复运行',
  stop: '停止运行',
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
  const { selectedGroup, setSelectedGroup, selectedRuns, setSelectedRuns } =
    useModel('operator');
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
  const [checked, setChecked] = useState<Row>();
  const [ticket, setTicket] = useState<Row>();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [receipt, setReceipt] = useState<Row>();
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
    setChecked(undefined);
    setTicket(undefined);
    setReceipt(undefined);
    setError('');
  }, [selectedGroup]);

  const group = groups.find((item) => item.id === selectedGroup);
  const control = runtime?.groups?.find(
    (item: Row) => item.groupId === selectedGroup,
  );
  const run = control?.runs?.find((item: Row) => item.runId === selectedRun);
  const legacy = !selectedRun && group?.runId ? group : undefined;
  const observedRun = run || legacy;
  const capability = control?.capabilities || {};
  const live = control?.kind === 'live';
  const effectiveBudget = budget ?? Number(control?.defaultBudgetUsdt || 2000);
  const inputs = live
    ? { durationSeconds: (minutes || 0) * 60 }
    : {
        budgetUsdt: effectiveBudget,
        durationSeconds: (minutes || 0) * 60,
      };
  const fingerprint = JSON.stringify([actionScope, inputs]);
  const validCheck =
    checked?.fingerprint === fingerprint && checked?.ok !== false;
  const eligible = admin && !!control && runtime?.available && !busy;
  const controllable = eligible && run && run.runId === control?.runId;
  const canStopLegacy = admin && !busy && legacy?.status === 'running';
  const eligibilityReason = !admin
    ? '当前账户为只读，请使用操作员账户'
    : busy
      ? '上一项请求正在处理'
      : !runtime
        ? '正在读取运行服务状态'
        : !runtime.available
          ? runtime.reason || '运行服务暂不可用'
          : !control
            ? '该策略组未配置运行服务'
            : '';
  const newRunReason =
    eligibilityReason ||
    (control?.nodeCompatibility?.ready === false
      ? control.nodeCompatibility.reason || '当前节点缺少模型需要的运行资源'
      : '') ||
    (!capability.start
      ? capability.reason || '该策略组当前不允许新建实例'
      : '');
  const runReason =
    eligibilityReason ||
    (!run
      ? '请先选择一个运行实例'
      : run.runId !== control?.runId
        ? '此实例已结束或不是当前活动实例'
        : '');
  const actionReasons = {
    halt:
      runReason ||
      (!capability.halt
        ? run?.status === 'halted'
          ? '此实例已暂停开仓'
          : '当前状态不支持暂停开仓'
        : ''),
    reduce:
      runReason ||
      (!capability.reduce
        ? run?.status === 'reducing'
          ? '此实例已处于仅减仓状态'
          : '当前状态不支持仅减仓'
        : ''),
    resume:
      runReason || (!capability.resume ? '仅已暂停或仅减仓的实例可恢复' : ''),
    stop: canStopLegacy
      ? ''
      : runReason || (!capability.stop ? '当前状态没有可停止的运行进程' : ''),
  };
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

  async function prepare(action: keyof typeof ACTIONS) {
    setBusy(true);
    setError('');
    const preparingScope = actionScope;
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
              },
            },
      );
      if (currentActionScope.current === preparingScope)
        setTicket({ ...result, actionScope: preparingScope });
    } catch (reason) {
      if (currentActionScope.current === preparingScope)
        setError(String(reason));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="strategy-run-panel">
      <label className="run-field" htmlFor="run-group">
        <span>策略组</span>
        <Select
          id="run-group"
          aria-label="运行策略组"
          value={selectedGroup}
          onChange={setSelectedGroup}
          showSearch={{ optionFilterProp: 'label' }}
          options={groups.map((item) => ({ value: item.id, label: item.name }))}
        />
      </label>
      <label className="run-field" htmlFor="inspector-run">
        <span>运行实例</span>
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
                ? `历史 · ${group.runId}`
                : control?.runs?.length
                  ? `未选择 · 共 ${control.runs.length} 个实例`
                  : '尚无运行实例',
            },
            ...(control?.runs || []).map((item: Row) => ({
              value: item.runId,
              label: `${item.runId} · ${STATES[item.status] || item.status}`,
            })),
          ]}
        />
      </label>
      <section className="run-section" aria-label="当前运行状态与预算">
        <h3>当前运行状态</h3>
        <dl className="run-facts">
          <div>
            <dt>执行环境</dt>
            <dd>
              {control
                ? control.mode === 'live'
                  ? 'OKX 实盘'
                  : control.mode === 'shadow'
                  ? '影子运行'
                  : 'Nautilus 本地模拟'
                : group?.modeLabel || '未配置'}
            </dd>
          </div>
          <div>
            <dt>运行状态</dt>
            <dd>
              {observedRun
                ? STATES[observedRun.status] || observedRun.status
                : control?.runs?.length
                  ? `尚未选择实例 · 共 ${control.runs.length} 个`
                  : '尚无运行实例'}
            </dd>
          </div>
          <div>
            <dt>模型 / 信号</dt>
            <dd>{control?.name || group?.description || '—'}</dd>
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
                : observedRun
                ? `${number(observedRun.budgetUsdt ?? observedRun.metrics?.capital)} USDT`
                : '未选择实例'}
            </dd>
          </div>
          <div>
            <dt>实例计划时长</dt>
            <dd>
              {observedRun?.durationSeconds
                ? `${number(observedRun.durationSeconds / 60)} 分钟`
                : '未记录'}
            </dd>
          </div>
          <div>
            <dt>资金占用</dt>
            <dd>
              {observedRun?.budgetWindow?.usedUsdt === undefined
                ? '待上报'
                : `${number(observedRun.budgetWindow.usedUsdt)} USDT`}
            </dd>
          </div>
        </dl>
        {observedRun?.status === 'halted' && (
          <p className="run-detail">已暂停开仓，可按需恢复运行。</p>
        )}
        <div className="run-actions run-control-actions">
          {(['halt', 'reduce', 'resume', 'stop'] as const).map((action) => (
            <div className="run-action" key={action}>
              <Button
                block
                danger={action === 'stop'}
                aria-describedby={
                  actionReasons[action] ? `run-${action}-reason` : undefined
                }
                disabled={
                  action === 'stop'
                    ? !canStopLegacy && (!controllable || !capability.stop)
                    : !controllable || !capability[action]
                }
                onClick={() => void prepare(action)}
              >
                {ACTIONS[action]}
              </Button>
              {actionReasons[action] && (
                <small id={`run-${action}-reason`}>
                  {actionReasons[action]}
                </small>
              )}
            </div>
          ))}
        </div>
      </section>
      <section className="run-section" aria-label="新建独立运行实例">
        <h3>新建运行实例</h3>
        <p className="run-detail">
          {live
            ? control?.exposureCapUsdt
              ? `只运行已发布的受限测试，新增净敞口上限 ${number(control.exposureCapUsdt)} USDT。`
              : '资金范围读取当前子账户库存。'
            : '设置新实例的独立预算与运行时长。'}
        </p>
        <div className="run-inputs">
          {!live && (
            <label className="run-field" htmlFor="run-budget">
              <span>新实例预算 / USDT</span>
              <InputNumber
                id="run-budget"
                aria-label="虚拟预算 USDT"
                value={effectiveBudget}
                min={0.01}
                max={Number(control?.maxBudgetUsdt || 2000)}
                precision={2}
                disabled={!!newRunReason}
                onChange={setBudget}
              />
            </label>
          )}
          <label className="run-field" htmlFor="run-minutes">
            <span>运行时长 / 分钟</span>
            <InputNumber
              id="run-minutes"
              aria-label="运行时长 分钟"
              value={minutes}
              min={1}
              precision={0}
              max={Math.floor((control?.maxDurationSeconds || 86400) / 60)}
              disabled={!!newRunReason}
              onChange={setMinutes}
            />
          </label>
        </div>
        <div className="run-actions">
          <Button
            block
            disabled={!!newRunReason}
            aria-describedby={newRunReason ? 'run-check-reason' : undefined}
            loading={busy}
            onClick={() => void preflight()}
          >
            检查配置
          </Button>
          {newRunReason && <small id="run-check-reason">{newRunReason}</small>}
          <Button
            block
            type="primary"
            disabled={!!newRunReason || !validCheck}
            aria-describedby={
              newRunReason || !validCheck ? 'run-start-reason' : undefined
            }
            onClick={() => void prepare('start')}
          >
            开始运行
          </Button>
          {(newRunReason || !validCheck) && (
            <small id="run-start-reason">
              {newRunReason || '请先检查当前预算、时长与策略版本'}
            </small>
          )}
        </div>
        {validCheck && (
          <p className="run-check-result">
            {live
              ? `${control?.exposureCapUsdt ? `新增净敞口 ≤ ${number(control.exposureCapUsdt)} USDT · ` : ''}${checked?.liveInventory?.pairs?.length || 0} 个交易对 · ${minutes} 分钟`
              : `配置已核验 · ${number(effectiveBudget)} USDT · ${minutes} 分钟`}
          </p>
        )}
      </section>
      {(runtime?.reason || capability.reason) && (
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
      {observedRun && (
        <section className="run-section">
          <h3>模型运行</h3>
          <ModelRuntime
            model={observedRun.engine?.model ?? observedRun.model}
          />
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

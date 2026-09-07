import { Alert, Descriptions, Modal } from 'antd';
import { useEffect, useRef, useState } from 'react';
import { strategyDisplayName } from '../pages/StrategyGroups/groupModel';
import { api, type Row } from './api';
import { operationLabel } from './labels';
import { OperationProgress } from './OperationProgress';
import { RecordDetails } from './RecordDetails';
export function ConfirmOperation({
  ticket,
  onClose,
  onComplete,
}: {
  ticket?: Row;
  onClose: () => void;
  onComplete: (result: Row) => void;
}) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [now, setNow] = useState(Date.now);
  const dismissed = useRef(new Set<string>());
  const executing = useRef<string | undefined>(undefined);
  const currentTicket = useRef(ticket);
  currentTicket.current = ticket;
  function close() {
    if (ticket?.id) dismissed.current.add(ticket.id);
    onClose();
  }
  useEffect(() => {
    setError('');
    setNow(Date.now());
    if (!ticket) return;
    const timer = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, [ticket]);
  const expiry = Number(ticket?.expiresAt);
  const remaining = Number.isFinite(expiry)
    ? Math.max(0, Math.ceil((expiry * 1000 - now) / 1000))
    : undefined;
  const expired = remaining === 0;
  const changingAccount =
    ticket?.operation?.kind === 'profile' && ticket.operation.name === 'select';
  const savingAccount =
    ticket?.operation?.kind === 'profile' && ticket.operation.name === 'save';
  const selectedProfile = ticket?.profile?.find((profile: Row) =>
    changingAccount
      ? profile.id === ticket?.operation?.arguments?.id
      : profile.active,
  );
  const active = savingAccount
    ? { ...selectedProfile, ...ticket?.operation?.arguments }
    : selectedProfile;
  async function execute() {
    if (
      !ticket?.id ||
      dismissed.current.has(ticket.id) ||
      currentTicket.current?.id !== ticket.id ||
      executing.current === ticket.id ||
      busy ||
      (Number.isFinite(expiry) && expiry * 1000 <= Date.now())
    )
      return;
    executing.current = ticket.id;
    setBusy(true);
    setError('');
    try {
      const result = await api('execute', {
        id: ticket?.id,
        ...(ticket.operation?.kind === 'group' &&
        ticket.operation?.name === 'start'
          ? { async: true }
          : {}),
      });
      onComplete(result);
      close();
    } catch (reason) {
      setError(String(reason));
    } finally {
      setBusy(false);
      executing.current = undefined;
    }
  }
  return (
    <Modal
      open={!!ticket}
      onCancel={close}
      keyboard={!busy}
      closable={!busy}
      mask={{ closable: false }}
      title={ticket ? `确认${operationLabel(ticket.operation)}` : '确认'}
      width={560}
      confirmLoading={busy}
      okText="确认"
      okButtonProps={{ disabled: expired }}
      cancelText="取消"
      cancelButtonProps={{ disabled: busy }}
      onOk={() => void execute()}
      destroyOnHidden
    >
      <div>
        {busy && (
          <OperationProgress
            label={
              ticket?.operation?.name === 'start'
                ? '提交启动请求'
                : '正在处理操作'
            }
          />
        )}
        {!busy && remaining !== undefined && (
          <Alert
            type={expired ? 'warning' : 'info'}
            title={
              expired
                ? '确认已过期，请取消并重新发起操作'
                : `确认有效期剩余 ${remaining} 秒`
            }
          />
        )}
        <div className="confirmation-context">
          <strong>
            {ticket?.operation?.kind === 'model_release'
              ? ticket.operation.arguments.releaseId ||
                ticket.operation.arguments.artifactId
              : ticket?.operation?.kind === 'group'
                ? strategyDisplayName(ticket.groupPreview?.groupName)
                : ticket?.operation?.kind === 'native'
                  ? ticket.operation.arguments.runId
                  : active?.name}
          </strong>
          <span className="environment">
            {ticket?.operation?.kind === 'model_release'
              ? '模型版本管理'
              : ticket?.operation?.kind === 'group'
                ? ticket.groupPreview?.mode === 'live'
                  ? 'OKX 实盘 · 订单启用'
                  : '研究运行'
                : ticket?.operation?.kind === 'native'
                  ? '历史运行'
                  : active?.mode === 'live'
                    ? 'OKX 实盘'
                    : '实盘 · 查看'}
          </span>
        </div>
        {ticket?.groupPreview && <p>{ticket.groupPreview.effect}</p>}
        {ticket?.operation?.kind === 'model_release' && (
          <Alert
            type="info"
            title={
              ticket.operation.name === 'validate'
                ? '校验模型文件与配置'
                : '更新模型发布版本'
            }
            description="启动入口：策略组。请设置预算与时长后确认运行。"
          />
        )}
        {ticket?.newAccountVerification && (
          <Descriptions
            size="small"
            column={1}
            items={[
              {
                key: 'account',
                label: '账户',
                children: ticket.newAccountVerification.uid,
              },
              {
                key: 'environment',
                label: '环境',
                children:
                  ticket.newAccountVerification.environment === 'live'
                    ? '实盘 · 交易'
                    : '实盘 · 查看',
              },
            ]}
          />
        )}
        {ticket?.groupPreview ? (
          <>
            <Descriptions
              size="small"
              column={1}
              items={[
                {
                  key: 'group',
                  label: '策略组',
                  children: strategyDisplayName(ticket.groupPreview.groupName),
                },
                {
                  key: 'action',
                  label: '操作',
                  children: operationLabel(ticket.operation),
                },
                {
                  key: 'run',
                  label: '目标实例',
                  children: ticket.operation.arguments.runId || '新建独立实例',
                },
                ...(ticket.operation.arguments.instruments
                  ? [
                      {
                        key: 'scope',
                        label: '平仓品种',
                        children:
                          ticket.operation.arguments.instruments.join('、'),
                      },
                    ]
                  : []),
                ...(ticket.operation.name === 'start'
                  ? [
                      ...(ticket.operation.arguments.budgetUsdt === undefined
                        ? []
                        : [
                            {
                              key: 'budget',
                              label: '运行预算',
                              children: `${ticket.operation.arguments.budgetUsdt} USDT`,
                            },
                          ]),
                      {
                        key: 'duration',
                        label: '运行时长',
                        children:
                          ticket.operation.arguments.durationSeconds === 0
                            ? '持续运行 · 手动停止'
                            : `${Number(ticket.operation.arguments.durationSeconds) / 60} 分钟`,
                      },
                      ...(ticket.groupPreview?.executionPolicy
                        ? [
                            {
                              key: 'execution-policy',
                              label: '策略执行方式',
                              children:
                                ticket.groupPreview.executionPolicy.label,
                            },
                          ]
                        : []),
                    ]
                  : []),
                ...(ticket.groupPreview?.liveInventory
                  ? [
                      ...(ticket.groupPreview.exposureCapUsdt
                        ? [
                            {
                              key: 'exposure-cap',
                              label: '新增净敞口上限',
                              children: `${ticket.groupPreview.exposureCapUsdt} USDT`,
                            },
                          ]
                        : []),
                      {
                        key: 'account-capital',
                        label: '账户总权益',
                        children: `${ticket.groupPreview.liveInventory.totalEqUsd} USD`,
                      },
                      ...(ticket.groupPreview.liveInventory
                        .allocationCapitalUsdt
                        ? [
                            {
                              key: 'strategy-capital',
                              label: '本组资金 / USDT',
                              children:
                                ticket.groupPreview.liveInventory
                                  .allocationCapitalUsdt,
                            },
                          ]
                        : []),
                      {
                        key: 'live-pairs',
                        label: '交易范围',
                        children: ticket.groupPreview.liveInventory.pairs
                          ?.map((row: Row) => row.instrument)
                          .join('、'),
                      },
                    ]
                  : []),
                {
                  key: 'strategy',
                  label: '策略版本',
                  children:
                    ticket.groupPreview.strategyVersion?.slice(0, 12) ||
                    '未提供',
                },
                {
                  key: 'signal',
                  label: '信号版本',
                  children:
                    ticket.groupPreview.signalVersion?.slice(0, 12) || '未提供',
                },
              ]}
            />
            {ticket.groupPreview.liveInventory?.orderPlan?.length > 0 && (
              <section className="launch-order-plan" aria-label="初始调仓预览">
                <h3>初始调仓预览</h3>
                <div style={{ maxHeight: 220, overflow: 'auto' }}>
                  <table className="launch-plan-table">
                    <thead>
                      <tr>
                        <th>品种</th>
                        <th>方向</th>
                        <th>数量</th>
                        <th>预估 USDT</th>
                      </tr>
                    </thead>
                    <tbody>
                      {ticket.groupPreview.liveInventory.orderPlan.map(
                        (row: Row) => (
                          <tr key={row.instrument}>
                            <td>{row.instrument}</td>
                            <td>{row.side === 'buy' ? '买入' : '卖出'}</td>
                            <td>{row.quantity}</td>
                            <td>{Number(row.estimatedUsdt).toFixed(2)}</td>
                          </tr>
                        ),
                      )}
                    </tbody>
                  </table>
                </div>
              </section>
            )}
            <details>
              <summary>完整技术参数与版本</summary>
              <RecordDetails
                value={{
                  ...ticket.operation.arguments,
                  ...ticket.groupPreview,
                }}
              />
            </details>
          </>
        ) : Array.isArray(ticket?.operation?.arguments) ? (
          <pre className="json-output">
            {JSON.stringify(ticket?.operation.arguments, null, 2)}
          </pre>
        ) : (
          <RecordDetails value={ticket?.operation?.arguments || {}} />
        )}
        {ticket?.operation?.kind === 'profile' &&
          ticket.operation.name === 'delete' && <p>删除此 API 连接？</p>}
        {error && <Alert type="error" title={error} />}
      </div>
    </Modal>
  );
}

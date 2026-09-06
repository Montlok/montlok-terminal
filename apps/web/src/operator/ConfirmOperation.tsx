import { Alert, Descriptions, Modal } from 'antd';
import { useEffect, useState } from 'react';
import { api, type Row } from './api';
import { operationLabel } from './labels';
import { RecordDetails } from './ResultView';
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
  const active = ticket?.profile?.find((profile: Row) =>
    changingAccount
      ? profile.id === ticket?.operation?.arguments?.id
      : profile.active,
  );
  async function execute() {
    if (busy || (Number.isFinite(expiry) && expiry * 1000 <= Date.now()))
      return;
    setBusy(true);
    setError('');
    try {
      const result = await api('execute', { id: ticket?.id });
      onComplete(result);
      onClose();
    } catch (reason) {
      setError(String(reason));
    } finally {
      setBusy(false);
    }
  }
  return (
    <Modal
      open={!!ticket}
      onCancel={onClose}
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
        {remaining !== undefined && (
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
                ? ticket.groupPreview?.groupName
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
                  : 'Nautilus 本地模拟'
                : ticket?.operation?.kind === 'native'
                  ? '本地模拟'
                  : active?.mode === 'demo'
                    ? '模拟盘'
                    : '实盘 · 只读'}
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
                  ticket.newAccountVerification.environment === 'demo'
                    ? '模拟盘'
                    : '实盘 · 只读',
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
                  children: ticket.groupPreview.groupName,
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
                ...(ticket.operation.name === 'start'
                  ? [
                      ...(ticket.operation.arguments.budgetUsdt === undefined
                        ? []
                        : [{
                        key: 'budget',
                        label: '独立虚拟预算',
                        children: `${ticket.operation.arguments.budgetUsdt} USDT`,
                      }]),
                      {
                        key: 'duration',
                        label: '运行时长',
                        children: `${Number(ticket.operation.arguments.durationSeconds) / 60} 分钟`,
                      },
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
                        children: `${ticket.groupPreview.liveInventory.totalEqUsd} USDT 等值`,
                      },
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

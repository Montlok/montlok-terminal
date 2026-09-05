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
  useEffect(() => {
    setError('');
  }, [ticket]);
  const active = ticket?.profile?.find((profile: Row) => profile.active);
  async function execute() {
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
      cancelText="取消"
      cancelButtonProps={{ disabled: busy }}
      onOk={() => void execute()}
      destroyOnHidden
    >
      <div>
        <div className="confirmation-context">
          <strong>
            {ticket?.operation?.kind === 'native'
              ? ticket.operation.arguments.runId
              : active?.name}
          </strong>
          <span className="environment">
            {ticket?.operation?.kind === 'native'
              ? '本地模拟'
              : active?.mode === 'demo'
                ? '模拟盘'
                : '实盘 · 只读'}
          </span>
        </div>
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
                children: ticket.newAccountVerification.environment,
              },
            ]}
          />
        )}
        {Array.isArray(ticket?.operation?.arguments) ? (
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

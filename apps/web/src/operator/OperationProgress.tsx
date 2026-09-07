import { LoadingOutlined } from '@ant-design/icons';
import { useEffect, useState } from 'react';

export function OperationProgress({
  label,
  startedAt,
}: {
  label: string;
  startedAt?: number;
}) {
  const [start] = useState(() => startedAt || Date.now());
  const [now, setNow] = useState(Date.now);
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, []);
  return (
    <div className="operation-progress" role="status" aria-live="polite">
      <LoadingOutlined aria-hidden />
      <span>{label}</span>
      <span className="muted">
        {Math.max(0, Math.floor((now - start) / 1000))} 秒
      </span>
    </div>
  );
}

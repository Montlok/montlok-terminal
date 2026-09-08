import { EventEnvelope } from '@montlok/sdk/protocol';
import { Tabs } from 'antd';
import { useEffect, useMemo, useState } from 'react';
import { ensureSession } from './api';
import {
  decodeRelated,
  elapsedMs,
  eventFields,
  eventLabel,
  eventTime,
  sameRun,
} from './eventDetailModel';

export function EventInspector({ event }: { event?: EventEnvelope }) {
  const [tab, setTab] = useState('summary');
  const [history, setHistory] = useState<{
    id: string;
    events: EventEnvelope[];
    truncated: boolean;
  }>();
  const [error, setError] = useState('');
  useEffect(() => {
    setError('');
    if (!event) return;
    const controller = new AbortController();
    void ensureSession()
      .then(() =>
        fetch(
          `/api/v2/events/${encodeURIComponent(event.eventId)}/related?limit=500`,
          {
            credentials: 'same-origin',
            cache: 'no-store',
            signal: controller.signal,
          },
        ),
      )
      .then(async (response) => {
        if (!response.ok) throw new Error('关联历史暂时无法读取');
        return response.json();
      })
      .then((value) => {
        if (!controller.signal.aborted)
          setHistory({
            id: event.eventId,
            events: decodeRelated(value).filter((item) => sameRun(item, event)),
            truncated: value.truncated,
          });
      })
      .catch((reason) => {
        if (!controller.signal.aborted)
          setError(String(reason.message || reason));
      });
    return () => controller.abort();
  }, [event?.eventId]);
  const rows = history?.id === event?.eventId ? history?.events : undefined;
  const raw = useMemo(
    () =>
      tab === 'raw' && event
        ? JSON.stringify(EventEnvelope.toJSON(event), null, 2)
        : '',
    [tab, event],
  );
  return (
    <aside className="event-inspector" aria-label="事件详情">
      <div className="panel-heading">
        <strong>事件详情</strong>
        <span>{event ? eventLabel(event) : '选择一条记录'}</span>
      </div>
      {!event ? (
        <div className="empty-state">
          选择事件、委托或成交，查看字段与关联时间线
        </div>
      ) : (
        <Tabs
          size="small"
          activeKey={tab}
          onChange={setTab}
          items={[
            {
              key: 'summary',
              label: '摘要与来源',
              children: (
                <dl className="event-field-list">
                  {eventFields(event).map(([label, value]) => (
                    <div key={label}>
                      <dt>{label}</dt>
                      <dd title={value}>{value || '—'}</dd>
                    </div>
                  ))}
                </dl>
              ),
            },
            {
              key: 'timeline',
              label: '关联时间线',
              children: (
                <div className="event-timeline">
                  {error ? (
                    <p role="status">{error}。当前记录仍可查看。</p>
                  ) : !rows ? (
                    <p>正在查询关联历史</p>
                  ) : null}
                  {rows?.map((item, index) => (
                    <div
                      className={
                        item.eventId === event.eventId ? 'selected' : ''
                      }
                      key={item.eventId}
                    >
                      <time>{eventTime(item.occurredAtNs)}</time>
                      <strong>{eventLabel(item)}</strong>
                      <span>
                        {index
                          ? `+ ${elapsedMs(rows[index - 1].occurredAtNs, item.occurredAtNs)} ms`
                          : '起点'}
                      </span>
                      <small>
                        {item.source} · #{item.streamSeq.toString()}
                      </small>
                    </div>
                  ))}
                  {history?.id === event.eventId && history.truncated && (
                    <p>关联记录超过 500 条，当前显示部分历史。</p>
                  )}
                  {rows?.length === 1 && <p>当前索引仅包含这条关联记录。</p>}
                </div>
              ),
            },
            {
              key: 'raw',
              label: '原始字段',
              children: <pre className="event-raw">{raw}</pre>,
            },
          ]}
        />
      )}
    </aside>
  );
}

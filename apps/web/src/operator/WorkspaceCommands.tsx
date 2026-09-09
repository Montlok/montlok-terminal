import { SearchOutlined } from '@ant-design/icons';
import { history, useModel } from '@umijs/max';
import { Button, Input, Modal } from 'antd';
import { useEffect, useMemo, useRef, useState } from 'react';
import { breadcrumbs, pages } from './navigation';
import { useRunObservation } from './runObservation';
import { strategyInstruments } from './strategyMarketModel';

export const TERMINAL_WORKSPACES = [
  { name: '实盘运行', path: '/strategies/groups/overview' },
  { name: '执行调查', path: '/workspace/execution/overview' },
  { name: '研究与发布', path: '/strategies/resources/models' },
  { name: '组合与风险', path: '/workspace/portfolio/overview' },
  { name: '行情与数据', path: '/market/prices/quotes' },
  { name: '运维与安全', path: '/engine/run/health' },
];
const commands = pages.map((page) => ({
  ...page,
  key: page.path,
  instrument: undefined as string | undefined,
  label: breadcrumbs(page.path).join(' / '),
}));

/** Navigation only. Trading commands retain their contextual confirmation forms. */
export function WorkspaceCommands({
  pathname,
  onEvents,
  onFocus,
}: {
  pathname: string;
  onEvents: () => void;
  onFocus: () => void;
}) {
  const [open, setOpen] = useState(false),
    [query, setQuery] = useState(''),
    [active, setActive] = useState(0);
  const list = useRef<HTMLDivElement>(null);
  const { selectedGroup, selectedRuns, setSelectedInstrument } =
    useModel('operator');
  const observation = useRunObservation(
    selectedGroup,
    selectedRuns?.[selectedGroup] || '',
    open,
  );
  const library = useMemo(
    () => [
      ...commands,
      ...strategyInstruments(observation.data).map((row) => ({
        key: `instrument:${row.instrument}`,
        path: row.instrument,
        label: `品种 / ${row.instrument}`,
        instrument: row.instrument,
      })),
      ...(observation.data?.runId
        ? [
            {
              key: `run:${observation.data.runId}`,
              path: '/strategies/groups/overview',
              label: `运行 / ${observation.data.runId}`,
              instrument: undefined,
            },
          ]
        : []),
    ],
    [observation.data],
  );
  const activate = (command: { path: string; instrument?: string }) => {
    if (command.instrument) setSelectedInstrument(command.instrument);
    else history.push(command.path);
    setOpen(false);
    setQuery('');
    setActive(0);
  };
  const matches = useMemo(
    () =>
      library.filter((command) =>
        `${command.label} ${command.path}`
          .toLowerCase()
          .includes(query.trim().toLowerCase()),
      ),
    [query, library],
  );
  const close = () => {
    setOpen(false);
    setQuery('');
    setActive(0);
  };
  useEffect(() => {
    const key = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') {
        event.preventDefault();
        setOpen((value) => !value);
      }
    };
    window.addEventListener('keydown', key);
    return () => window.removeEventListener('keydown', key);
  }, []);
  useEffect(() => {
    list.current
      ?.querySelector('[aria-selected="true"]')
      ?.scrollIntoView?.({ block: 'nearest' });
  }, [active]);
  return (
    <div className="terminal-workspace-bar">
      <nav aria-label="终端工作区">
        {TERMINAL_WORKSPACES.map((item) => (
          <button
            key={item.path}
            type="button"
            aria-current={pathname === item.path ? 'page' : undefined}
            onClick={() => history.push(item.path)}
          >
            {item.name}
          </button>
        ))}
      </nav>
      <Button type="text" onClick={onEvents}>
        事件与订单
      </Button>
      <Button type="text" onClick={onFocus}>
        聚焦面板
      </Button>
      <Button
        className="terminal-command-trigger"
        icon={<SearchOutlined />}
        onClick={() => setOpen(true)}
      >
        查找功能 <kbd>⌘ / Ctrl K</kbd>
      </Button>
      <Modal
        open={open}
        onCancel={close}
        footer={null}
        title="查找功能"
        width={620}
        destroyOnHidden
        afterOpenChange={(visible) => {
          if (visible)
            document
              .querySelector<HTMLInputElement>('[aria-label="功能搜索"]')
              ?.focus();
        }}
      >
        <Input
          aria-label="功能搜索"
          placeholder="输入功能、品种或运行实例"
          value={query}
          onChange={(event) => {
            setQuery(event.target.value);
            setActive(0);
          }}
          onKeyDown={(event) => {
            if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
              event.preventDefault();
              setActive((value) =>
                Math.max(
                  0,
                  Math.min(
                    matches.length - 1,
                    value + (event.key === 'ArrowDown' ? 1 : -1),
                  ),
                ),
              );
            }
            if (event.key === 'Enter' && matches[active]) {
              activate(matches[active]);
            }
          }}
          role="combobox"
          aria-expanded="true"
          aria-controls="terminal-command-results"
          aria-activedescendant={
            matches[active] ? `terminal-command-${active}` : undefined
          }
        />
        <div
          id="terminal-command-results"
          className="terminal-command-results"
          ref={list}
          role="listbox"
          aria-label="功能搜索结果"
        >
          {matches.map((item, index) => (
            <button
              type="button"
              role="option"
              id={`terminal-command-${index}`}
              aria-selected={active === index}
              key={item.key}
              onMouseEnter={() => setActive(index)}
              onClick={() => {
                activate(item);
              }}
            >
              <span>{item.label}</span>
              <small>{item.path}</small>
            </button>
          ))}
          {!matches.length && <p>没有匹配的功能</p>}
        </div>
      </Modal>
    </div>
  );
}

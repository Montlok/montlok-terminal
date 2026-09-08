import { SearchOutlined } from '@ant-design/icons';
import { history } from '@umijs/max';
import { Button, Input, Modal } from 'antd';
import { useEffect, useMemo, useRef, useState } from 'react';
import { breadcrumbs, pages } from './navigation';

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
  const matches = useMemo(
    () =>
      commands.filter((command) =>
        `${command.label} ${command.path}`
          .toLowerCase()
          .includes(query.trim().toLowerCase()),
      ),
    [query],
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
          placeholder="输入功能、品种类别或页面名称"
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
              history.push(matches[active].path);
              close();
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
              key={item.path}
              onMouseEnter={() => setActive(index)}
              onClick={() => {
                history.push(item.path);
                close();
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

import { history, useLocation } from '@umijs/max';
import { Button, Splitter, Tabs } from 'antd';
import { lazy, type ReactNode, Suspense, useEffect, useState } from 'react';
import {
  closeWorkspaceTab,
  loadWorkspaceTabs,
  openWorkspaceTab,
  PINNED_WORKSPACE_TABS,
  resizedWorkspaceSplit,
  WORKSPACE_SPLIT_KEY,
  WORKSPACE_TABS_KEY,
  workspaceSplit,
  workspaceTabLabel,
} from './quantWorkspaceModel';
import './workspace.css';
import { StrategyRunPanel } from './StrategyRunPanel';

const MarketDock = lazy(() =>
  import('./MarketDock').then((module) => ({ default: module.MarketDock })),
);

function saved(key: string): string | null {
  try {
    return window.localStorage.getItem(key);
  } catch {
    return null;
  }
}

function save(key: string, value: string) {
  try {
    window.localStorage.setItem(key, value);
  } catch {
    // Layout preferences are optional when browser storage is unavailable.
  }
}

/** The market remains mounted; only the lower routed workspace changes. */
export function QuantWorkspace({ children }: { children: ReactNode }) {
  const { pathname } = useLocation();
  const [paths, setPaths] = useState(() =>
    openWorkspaceTab(loadWorkspaceTabs(saved(WORKSPACE_TABS_KEY)), pathname),
  );
  const [split, setSplit] = useState(() =>
    workspaceSplit(saved(WORKSPACE_SPLIT_KEY)),
  );
  const [compact, setCompact] = useState(() => {
    const preference = saved('montlok.workspace.market.compact.v2');
    return preference === null
      ? pathname.startsWith('/settings/') || pathname.startsWith('/assets/')
      : preference === 'true';
  });
  const toggleMarket = () =>
    setCompact((previous) => {
      save('montlok.workspace.market.compact.v2', String(!previous));
      return !previous;
    });
  const visiblePaths = openWorkspaceTab(paths, pathname);
  const [inspectorOpen, setInspectorOpen] = useState(
    () => saved('montlok.workspace.controls.v1') !== 'false',
  );
  useEffect(() => save('montlok.workspace.controls.v1', String(inspectorOpen)), [inspectorOpen]);

  useEffect(() => {
    setPaths((current) => openWorkspaceTab(current, pathname));
  }, [pathname]);
  useEffect(() => save(WORKSPACE_TABS_KEY, JSON.stringify(paths)), [paths]);

  return (
    <div
      className={`quant-workspace quant-workspace-focus ${inspectorOpen ? 'inspector-open' : ''}`}
    >
      <div className="workspace-main">
        <Splitter
          orientation="vertical"
          className="quant-workspace-splitter"
          onResize={(sizes) => {
            if (compact) return;
            const next = resizedWorkspaceSplit(sizes);
            if (next !== undefined) setSplit(next);
          }}
          onResizeEnd={(sizes) => {
            if (compact) return;
            const next = resizedWorkspaceSplit(sizes);
            if (next === undefined) return;
            setSplit(next);
            save(WORKSPACE_SPLIT_KEY, String(next));
          }}
          onDraggerDoubleClick={() => {
            setSplit(58);
            save(WORKSPACE_SPLIT_KEY, '58');
          }}
        >
          <Splitter.Panel
            size={compact ? 56 : `${split}%`}
            min={compact ? 56 : '28%'}
            max={compact ? 56 : '65%'}
            resizable={!compact}
          >
            <Suspense
              fallback={<div className="workspace-loading">加载行情</div>}
            >
              <MarketDock compact={compact} onToggle={toggleMarket} />
            </Suspense>
          </Splitter.Panel>
          <Splitter.Panel min="35%">
            <section className="workspace-editor" aria-label="功能工作区">
              <Tabs
                className="workspace-route-tabs"
                type="editable-card"
                size="small"
                hideAdd
                tabBarExtraContent={
                  <Button
                    className="workspace-inspector-toggle"
                    type={inspectorOpen ? 'default' : 'primary'}
                    aria-expanded={inspectorOpen}
                    aria-controls="workspace-strategy-controls"
                    onClick={() => setInspectorOpen(!inspectorOpen)}
                  >
                    策略控制
                  </Button>
                }
                activeKey={pathname}
                onChange={(path) => history.push(path)}
                onEdit={(key, action) => {
                  if (action !== 'remove' || typeof key !== 'string') return;
                  const next = closeWorkspaceTab(visiblePaths, key, pathname);
                  setPaths(next.paths);
                  if (next.active !== pathname) history.push(next.active);
                }}
                items={visiblePaths.map((path) => ({
                  key: path,
                  label: workspaceTabLabel(path),
                  closable: !PINNED_WORKSPACE_TABS.includes(path),
                }))}
              />
              <div className="workspace-route-content">{children}</div>
            </section>
          </Splitter.Panel>
        </Splitter>
      </div>
      <aside id="workspace-strategy-controls" className="workspace-inspector" aria-label="策略控制">
        <div className="panel-heading">
          <strong>策略控制</strong>
          <Button
            className="workspace-inspector-toggle"
            type="text"
            onClick={() => setInspectorOpen(false)}
          >
            收起
          </Button>
        </div>
        <StrategyRunPanel />
      </aside>
    </div>
  );
}

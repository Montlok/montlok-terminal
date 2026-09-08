import { history, useLocation } from '@umijs/max';
import { Button, Tabs } from 'antd';
import { Actions, Layout, type Model, type TabNode } from 'flexlayout-react';
import { lazy, type ReactNode, Suspense, useEffect, useState } from 'react';
import {
  closeWorkspaceTab, loadWorkspaceTabs, openWorkspaceTab,
  PINNED_WORKSPACE_TABS, WORKSPACE_TABS_KEY, workspaceTabLabel,
} from './quantWorkspaceModel';
import { DOCKING_LAYOUT_KEY, loadDockingModel } from './dockingModel';
import { StrategyRunPanel } from './StrategyRunPanel';
import 'flexlayout-react/style/dark.css';
import './workspace.css';
import './terminalDocking.css';

const MarketDock = lazy(() => import('./MarketDock').then((module) => ({ default: module.MarketDock })));
const EventBlotter=lazy(()=>import('./EventBlotter'));
function saved(key: string): string | null {
  try { return window.localStorage.getItem(key); } catch { return null; }
}
function save(key: string, value: string) {
  try { window.localStorage.setItem(key, value); } catch { /* Optional layout preference. */ }
}

/** Docking changes observation and geometry only; writes belong to StrategyRunPanel. */
export function QuantWorkspace({ children }: { children: ReactNode }) {
  const { pathname } = useLocation();
  const [paths, setPaths] = useState(() => openWorkspaceTab(loadWorkspaceTabs(saved(WORKSPACE_TABS_KEY)), pathname));
  const [model] = useState(() => loadDockingModel(saved(DOCKING_LAYOUT_KEY)));
  const [compact, setCompact] = useState(() => saved('montlok.workspace.market.compact.v2') === 'true');
  const toggleMarket = () => setCompact((previous) => {
    save('montlok.workspace.market.compact.v2', String(!previous));
    return !previous;
  });
  const visiblePaths = openWorkspaceTab(paths, pathname);
  useEffect(() => setPaths((current) => openWorkspaceTab(current, pathname)), [pathname]);
  useEffect(() => save(WORKSPACE_TABS_KEY, JSON.stringify(paths)), [paths]);

  const factory = (node: TabNode) => {
    switch (node.getComponent()) {
      case 'events':
        return <Suspense fallback={<div className="workspace-loading">加载运行事件</div>}><EventBlotter/></Suspense>;
      case 'market':
        return <div className="workspace-main"><Suspense fallback={<div className="workspace-loading">加载行情</div>}>
          <MarketDock compact={compact} onToggle={toggleMarket} />
        </Suspense></div>;
      case 'controls':
        return <aside id="workspace-strategy-controls" className="workspace-inspector" aria-label="策略控制"><StrategyRunPanel /></aside>;
      case 'workspace':
        return <section className="workspace-editor" aria-label="功能工作区">
          <Tabs className="workspace-route-tabs" type="editable-card" size="small" hideAdd
            tabBarExtraContent={<Button onClick={() => model.doAction(Actions.selectTab('controls'))}>策略控制</Button>}
            activeKey={pathname} onChange={(path) => history.push(path)}
            onEdit={(key, action) => {
              if (action !== 'remove' || typeof key !== 'string') return;
              const next = closeWorkspaceTab(visiblePaths, key, pathname);
              setPaths(next.paths);
              if (next.active !== pathname) history.push(next.active);
            }}
            items={visiblePaths.map((path) => ({ key: path, label: workspaceTabLabel(path), closable: !PINNED_WORKSPACE_TABS.includes(path) }))}
          />
          <div className="workspace-route-content">{children}</div>
        </section>;
      default: return null;
    }
  };

  return <div className="quant-workspace terminal-docking" aria-label="可停靠终端工作区">
    <Layout model={model} factory={factory} realtimeResize={false} popoutURL="/popout.html"
      onModelChange={(next: Model) => save(DOCKING_LAYOUT_KEY, JSON.stringify(next.toJson()))}
    />
  </div>;
}

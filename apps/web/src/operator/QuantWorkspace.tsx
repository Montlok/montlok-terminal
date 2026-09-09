import { history, useLocation } from '@umijs/max';
import {
  Actions,
  DockLocation,
  Layout,
  type Model,
  type TabNode,
} from 'flexlayout-react';
import {
  lazy,
  type ReactNode,
  Suspense,
  useEffect,
  useRef,
  useState,
} from 'react';
import {
  closeWorkspaceTab,
  loadWorkspaceTabs,
  openWorkspaceTab,
  WORKSPACE_TABS_KEY,
} from './quantWorkspaceModel';
import {
  DOCKING_LAYOUT_KEY,
  loadDockingModel,
  routeTab,
  routeTabId,
} from './dockingModel';
import { StrategyRunPanel } from './StrategyRunPanel';
import { WorkspaceCommands } from './WorkspaceCommands';
import 'flexlayout-react/style/dark.css';
import './workspace.css';
import './terminalDocking.css';
import {assetPath} from './assetPath';
const MarketDock = lazy(() =>
  import('./MarketDock').then((module) => ({ default: module.MarketDock })),
);
const EventBlotter = lazy(() => import('./EventBlotter'));
function saved(key: string) {
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
    /* Optional device preference. */
  }
}
/** A single tab layer. Navigation changes observation only; the control pane persists. */
export function QuantWorkspace({ children }: { children: ReactNode }) {
  const { pathname } = useLocation();
  const [model] = useState(() => loadDockingModel(saved(DOCKING_LAYOUT_KEY)));
  const [paths, setPaths] = useState(() =>
    openWorkspaceTab(loadWorkspaceTabs(saved(WORKSPACE_TABS_KEY)), pathname),
  );
  const closingPath = useRef<string | undefined>(undefined);
  const [compact, setCompact] = useState(
    () => saved('montlok.workspace.market.compact.v2') === 'true',
  );
  const toggleMarket = () =>
    setCompact((value) => {
      save('montlok.workspace.market.compact.v2', String(!value));
      return !value;
    });
  useEffect(() => {
    if (closingPath.current === pathname) return;
    closingPath.current = undefined;
    const next = openWorkspaceTab(paths, pathname);
    for (const path of next)
      if (!model.getNodeById(routeTabId(path)))
        model.doAction(
          Actions.addTab(
            routeTab(path),
            model.getNodeById('events')?.getParent()?.getId() ||
              'workspace-tabset',
            DockLocation.CENTER,
            -1,
            false,
          ),
        );
    if (model.getNodeById(routeTabId(pathname)))
      model.doAction(Actions.selectTab(routeTabId(pathname)));
    if (next !== paths) setPaths(next);
    save(WORKSPACE_TABS_KEY, JSON.stringify(next));
  }, [pathname, model, paths]);
  const factory = (node: TabNode) => {
    switch (node.getComponent()) {
      case 'events':
        return (
          <Suspense
            fallback={<div className="workspace-loading">加载运行事件</div>}
          >
            <EventBlotter />
          </Suspense>
        );
      case 'market':
        return (
          <div className="workspace-main">
            <Suspense
              fallback={<div className="workspace-loading">加载行情</div>}
            >
              <MarketDock compact={compact} onToggle={toggleMarket} />
            </Suspense>
          </div>
        );
      case 'controls':
        return (
          <aside
            id="workspace-strategy-controls"
            className="workspace-inspector"
            aria-label="策略控制"
          >
            <StrategyRunPanel />
          </aside>
        );
      case 'route':
        return node.getConfig()?.path === pathname ? (
          <section className="workspace-editor" aria-label="功能工作区">
            <div className="workspace-route-content">{children}</div>
          </section>
        ) : null;
      default:
        return null;
    }
  };
  return (
    <div
      className="quant-workspace terminal-docking"
      aria-label="可停靠终端工作区"
    >
      <WorkspaceCommands
        pathname={pathname}
        onEvents={() => model.doAction(Actions.selectTab('events'))}
        onFocus={() => {
          const id = model
            .getNodeById(routeTabId(pathname))
            ?.getParent()
            ?.getId();
          if (id) model.doAction(Actions.maximizeToggle(id));
        }}
      />
      <div className="terminal-docking-surface">
        <Layout
          model={model}
          factory={factory}
          realtimeResize={false}
          popoutURL={assetPath('popout.html')}
          onAction={(action) => {
            if (action.type === Actions.SELECT_TAB) {
              const node = model.getNodeById(action.data.tabNode) as
                | TabNode
                | undefined;
              const path =
                node?.getComponent() === 'route'
                  ? node.getConfig()?.path
                  : undefined;
              if (path && path !== pathname) history.push(path);
            }
            if (action.type === Actions.DELETE_TAB) {
              const node = model.getNodeById(action.data.node) as
                | TabNode
                | undefined;
              const path = node?.getConfig()?.path;
              if (path) {
                const next = closeWorkspaceTab(paths, path, pathname);
                if (next.active !== pathname) {
                  closingPath.current = pathname;
                  history.push(next.active);
                }
                setPaths(next.paths);
                save(WORKSPACE_TABS_KEY, JSON.stringify(next.paths));
              }
            }
            return action;
          }}
          onModelChange={(next: Model) =>
            save(DOCKING_LAYOUT_KEY, JSON.stringify(next.toJson()))
          }
        />
      </div>
    </div>
  );
}

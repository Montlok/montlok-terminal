import { Model, type IJsonModel, type IJsonTabNode } from 'flexlayout-react';
import { pageFor } from './navigation';
import {
  PINNED_WORKSPACE_TABS,
  workspaceTabLabel,
} from './quantWorkspaceModel';
export const DOCKING_LAYOUT_KEY = 'montlok.workspace.docking.v3';
export const routeTabId = (path: string) => `route:${path}`;
export function routeTab(path: string): IJsonTabNode {
  return {
    type: 'tab',
    id: routeTabId(path),
    name: workspaceTabLabel(path),
    component: 'route',
    config: { path },
    enableClose: !PINNED_WORKSPACE_TABS.includes(path),
    enableRenderOnDemand: true,
  };
}
export const DEFAULT_DOCKING_LAYOUT: IJsonModel = {
  global: {
    tabEnableClose: false,
    tabEnableRename: false,
    tabEnablePopout: true,
    tabSetEnableMaximize: true,
    tabSetMinWidth: 200,
    tabSetMinHeight: 100,
  },
  borders: [],
  layout: {
    type: 'row',
    children: [
      {
        type: 'row',
        id: 'analysis',
        weight: 78,
        children: [
          {
            type: 'tabset',
            id: 'market-tabset',
            weight: 55,
            children: [
              {
                type: 'tab',
                id: 'market',
                name: '行情与策略标的',
                component: 'market',
                enableRenderOnDemand: false,
              },
            ],
          },
          {
            type: 'tabset',
            id: 'workspace-tabset',
            weight: 45,
            children: [
              ...PINNED_WORKSPACE_TABS.map(routeTab),
              {
                type: 'tab',
                id: 'events',
                name: '事件与订单',
                component: 'events',
              },
            ],
          },
        ],
      },
      {
        type: 'tabset',
        id: 'controls-tabset',
        weight: 22,
        children: [
          {
            type: 'tab',
            id: 'controls',
            name: '运行上下文与控制',
            component: 'controls',
            enableRenderOnDemand: false,
          },
        ],
      },
    ],
  },
};
/** Saved geometry may only refer to registered views and known business routes. */
export function loadDockingModel(raw: string | null): Model {
  if (raw && raw.length < 256_000)
    try {
      const model = Model.fromJson(JSON.parse(raw) as IJsonModel);
      let valid = true;
      const ids = new Set<string>();
      const components: string[] = [];
      model.visitNodes((node) => {
        if (node.getType() !== 'tab') return;
        const tab = node.toJson() as IJsonTabNode;
        if (!tab.id || ids.has(tab.id)) valid = false;
        ids.add(tab.id || '');
        components.push(tab.component || '');
        if (tab.component === 'route') {
          const path = tab.config?.path;
          if (
            typeof path !== 'string' ||
            !pageFor(path) ||
            tab.id !== routeTabId(path)
          )
            valid = false;
        } else if (
          !['market', 'controls', 'events'].includes(tab.component || '')
        )
          valid = false;
      });
      if (
        valid &&
        ids.size <= 16 &&
        ['market', 'controls', 'events'].every(
          (name) => components.filter((c) => c === name).length === 1,
        ) &&
        components.includes('route')
      )
        return model;
    } catch {
      /* Invalid preferences do not remove access to business views. */
    }
  return Model.fromJson(DEFAULT_DOCKING_LAYOUT);
}

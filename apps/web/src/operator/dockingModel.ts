import { Model, type IJsonModel } from 'flexlayout-react';

export const DOCKING_LAYOUT_KEY = 'montlok.workspace.docking.v2';
export const DEFAULT_DOCKING_LAYOUT: IJsonModel = {
  global: {
    tabEnableClose: false, tabEnableRename: false, tabEnablePopout: true,
    tabSetEnableMaximize: true, tabSetMinWidth: 240, tabSetMinHeight: 100,
  },
  borders: [],
  layout: { type: 'row', children: [
    { type: 'row', id: 'analysis', weight: 78, children: [
      { type: 'tabset', id: 'market-tabset', weight: 52, children: [
        { type: 'tab', id: 'market', name: '行情与策略标的', component: 'market', enableRenderOnDemand: false },
      ] },
      { type: 'tabset', id: 'workspace-tabset', weight: 48, children: [
        { type: 'tab', id: 'workspace', name: '功能工作区', component: 'workspace', enableRenderOnDemand: false },
        { type:'tab', id:'events',name:'运行事件',component:'events',enableRenderOnDemand:true },
      ] },
    ] },
    { type: 'tabset', id: 'controls-tabset', weight: 22, children: [
      { type: 'tab', id: 'controls', name: '策略控制', component: 'controls', enableRenderOnDemand: false },
    ] },
  ] },
};

/** Layout JSON only describes known panels, never executable component URLs. */
export function loadDockingModel(raw: string | null): Model {
  if (raw && raw.length < 256_000) {
    try {
      const model = Model.fromJson(JSON.parse(raw) as IJsonModel);
      const components: string[] = [];
      model.visitNodes((node) => {
        if (node.getType() === 'tab') components.push(String((node.toJson() as {component?: string}).component));
      });
      if ((components.length===3||components.length===4) && ['market','workspace','controls'].every(name=>components.includes(name)) && components.every(name=>['market','workspace','controls','events'].includes(name))) return model;
    } catch { /* Invalid saved layouts use the shipped workspace. */ }
  }
  return Model.fromJson(DEFAULT_DOCKING_LAYOUT);
}

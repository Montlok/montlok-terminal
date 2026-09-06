import { describe, expect, it } from 'vitest';
import { pages } from './navigation';
import {
  closeWorkspaceTab,
  loadWorkspaceTabs,
  openWorkspaceTab,
  PINNED_WORKSPACE_TABS,
  resizedWorkspaceSplit,
  WORKSPACE_SPLIT_KEY,
  WORKSPACE_TAB_LIMIT,
  workspaceSplit,
  workspaceTabLabel,
} from './quantWorkspaceModel';

describe('quant workspace navigation', () => {
  it('keeps permanent portfolio, strategy and account panels and deduplicates routes', () => {
    expect(PINNED_WORKSPACE_TABS).toEqual([
      '/workspace/portfolio/overview',
      '/strategies/groups/overview',
      '/trade/spot/terminal',
    ]);
    const tabs = loadWorkspaceTabs(
      JSON.stringify(['/trade/spot/terminal', '/assets/account/overview']),
    );
    expect(tabs).toEqual([
      ...PINNED_WORKSPACE_TABS,
      '/assets/account/overview',
    ]);
    expect(openWorkspaceTab(tabs, '/trade/spot/terminal')).toBe(tabs);
    expect(
      closeWorkspaceTab(
        tabs,
        PINNED_WORKSPACE_TABS[0],
        '/assets/account/overview',
      ).paths,
    ).toBe(tabs);
  });
  it('accepts only actual internal pages from optional browser storage', () => {
    expect(loadWorkspaceTabs('{')).toEqual(PINNED_WORKSPACE_TABS);
    expect(
      loadWorkspaceTabs(
        JSON.stringify(['/login', 'https://example.com', 42, '/bad']),
      ),
    ).toEqual(PINNED_WORKSPACE_TABS);
    expect(loadWorkspaceTabs('null')).toEqual(PINNED_WORKSPACE_TABS);
  });
  it('bounds open tabs and never evicts any pinned workspace', () => {
    const tabs = pages.reduce(
      (tabs, page) => openWorkspaceTab(tabs, page.path),
      [...PINNED_WORKSPACE_TABS],
    );
    expect(tabs).toHaveLength(WORKSPACE_TAB_LIMIT);
    for (const path of PINNED_WORKSPACE_TABS) expect(tabs).toContain(path);
    expect(tabs.at(-1)).toBe(pages.at(-1)?.path);
  });
  it('closes an active page to its nearest left neighbor without changing another active page', () => {
    const paths = [
      ...PINNED_WORKSPACE_TABS,
      '/assets/account/overview',
      '/strategies/bots/grid',
    ];
    expect(
      closeWorkspaceTab(
        paths,
        '/strategies/bots/grid',
        '/strategies/bots/grid',
      ),
    ).toEqual({
      paths: paths.slice(0, -1),
      active: '/assets/account/overview',
    });
    expect(
      closeWorkspaceTab(
        paths,
        '/assets/account/overview',
        '/strategies/bots/grid',
      ).active,
    ).toBe('/strategies/bots/grid');
  });
  it('disambiguates repeated page names with their secondary navigation', () => {
    expect(workspaceTabLabel('/trade/spot/orders')).toBe('现货 · 委托');
    expect(workspaceTabLabel('/trade/swap/orders')).toBe('永续 · 委托');
  });
  it('keeps both panels available and rejects invalid saved splitter sizes', () => {
    expect(workspaceSplit(60)).toBe(60);
    expect(WORKSPACE_SPLIT_KEY).toBe('montlok.workspace.split.v3');
    expect(workspaceSplit('65')).toBe(65);
    expect(workspaceSplit(28)).toBe(28);
    for (const invalid of [
      null,
      undefined,
      -1,
      27,
      66,
      75,
      Number.NaN,
      'wrong',
    ]) {
      expect(workspaceSplit(invalid)).toBe(58);
    }
  });
  it('clamps resized fractions without jumping back to the default at a floating point boundary', () => {
    expect(resizedWorkspaceSplit([600, 400])).toBe(60);
    expect(resizedWorkspaceSplit([650.00001, 349.99999])).toBe(65);
    expect(resizedWorkspaceSplit([279.99999, 720.00001])).toBe(28);
    expect(resizedWorkspaceSplit([0, 0])).toBeUndefined();
    expect(resizedWorkspaceSplit([Number.NaN, 400])).toBeUndefined();
  });
});

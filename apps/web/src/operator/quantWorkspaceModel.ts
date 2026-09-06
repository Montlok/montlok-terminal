import { breadcrumbs, pageFor } from './navigation';

export const WORKSPACE_TABS_KEY = 'montlok.workspace.tabs.v1';
export const WORKSPACE_SPLIT_KEY = 'montlok.workspace.split.v3';
export const PINNED_WORKSPACE_TABS = [
  '/workspace/portfolio/overview',
  '/strategies/groups/overview',
  '/trade/spot/terminal',
];
export const WORKSPACE_TAB_LIMIT = 12;

export function workspaceTabLabel(path: string): string {
  return breadcrumbs(path).slice(-2).join(' · ') || pageFor(path)?.name || path;
}

export function openWorkspaceTab(paths: string[], path: string): string[] {
  if (!pageFor(path) || paths.includes(path)) return paths;
  const next = [...paths, path];
  if (next.length > WORKSPACE_TAB_LIMIT) {
    const oldest = next.findIndex(
      (item) => !PINNED_WORKSPACE_TABS.includes(item),
    );
    next.splice(oldest, 1);
  }
  return next;
}

export function loadWorkspaceTabs(raw: string | null): string[] {
  try {
    const parsed: unknown = JSON.parse(raw || '[]');
    if (!Array.isArray(parsed)) return [...PINNED_WORKSPACE_TABS];
    return parsed.reduce<string[]>(
      (tabs, item) => {
        return typeof item === 'string' ? openWorkspaceTab(tabs, item) : tabs;
      },
      [...PINNED_WORKSPACE_TABS],
    );
  } catch {
    return [...PINNED_WORKSPACE_TABS];
  }
}

export function closeWorkspaceTab(
  paths: string[],
  path: string,
  active: string,
) {
  if (PINNED_WORKSPACE_TABS.includes(path)) return { paths, active };
  const index = paths.indexOf(path);
  const next = paths.filter((item) => item !== path);
  return {
    paths: next,
    active:
      active === path
        ? next[Math.max(0, index - 1)] || PINNED_WORKSPACE_TABS[0]
        : active,
  };
}

export function workspaceSplit(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 28 && parsed <= 65 ? parsed : 58;
}

export function resizedWorkspaceSplit(sizes: number[]): number | undefined {
  const total = sizes.reduce((sum, size) => sum + size, 0);
  if (sizes.length !== 2 || !Number.isFinite(total) || total <= 0)
    return undefined;
  return Math.min(65, Math.max(28, (sizes[0] / total) * 100));
}

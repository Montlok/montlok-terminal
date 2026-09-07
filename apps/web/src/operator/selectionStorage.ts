import { api, type Row } from './api';

export type RunSelection = {
  selectedGroup: string;
  selectedRuns: Record<string, string>;
};

export const selectionKey = (operator: string) =>
  `operator.run-selection.v3:${encodeURIComponent(operator)}`;

export function readSelection(operator: string): RunSelection | undefined {
  try {
    const saved = JSON.parse(
      localStorage.getItem(selectionKey(operator)) || 'null',
    );
    if (!saved || typeof saved.selectedGroup !== 'string') return undefined;
    const selectedRuns: Record<string, string> = {};
    if (saved.selectedRuns && typeof saved.selectedRuns === 'object')
      for (const [group, run] of Object.entries(saved.selectedRuns))
        if (
          typeof run === 'string' &&
          run &&
          !['__proto__', 'constructor', 'prototype'].includes(group)
        )
          selectedRuns[group] = run;
    return { selectedGroup: saved.selectedGroup, selectedRuns };
  } catch {
    return undefined;
  }
}

export function saveSelection(operator: string, selection: RunSelection) {
  try {
    // Persist navigation identity only. Never store confirmations, actions, or input budgets.
    localStorage.setItem(
      selectionKey(operator),
      JSON.stringify({
        selectedGroup: selection.selectedGroup,
        selectedRuns: selection.selectedRuns,
      }),
    );
  } catch {
    // Browsers may disable local storage; the current session remains usable.
  }
}

export async function validateSelection(
  saved: RunSelection,
): Promise<RunSelection> {
  const listing = await api('strategy-groups');
  const groups: Row[] = listing.groups || [];
  if (!groups.length)
    return { selectedGroup: 'live-sector-6040', selectedRuns: {} };
  const selectedGroup = groups.some((group) => group.id === saved.selectedGroup)
    ? saved.selectedGroup
    : groups.find(
        (group) => group.id === 'live-sector-6040' && group.mode === 'live',
      )?.id ||
      groups.find((group) => group.mode === 'live')?.id ||
      groups.find((group) => group.id === 'baseline')?.id ||
      groups[0]?.id ||
      'baseline';
  const selectedRuns: Record<string, string> = {};
  await Promise.all(
    groups.map(async (group) => {
      const runId = saved.selectedRuns[group.id];
      if (!runId) return;
      try {
        const result = await api(
          `strategy-groups/${encodeURIComponent(group.id)}/runtime`,
        );
        const current = result.groups?.find(
          (item: Row) => item.groupId === group.id,
        );
        if (
          result.available &&
          current?.runs?.some(
            (run: Row) =>
              run.runId === runId && (!run.groupId || run.groupId === group.id),
          )
        )
          selectedRuns[group.id] = runId;
        else if (group.runId === runId) selectedRuns[group.id] = '';
      } catch {
        // Unverified runs are not restored as action targets.
      }
    }),
  );
  return { selectedGroup, selectedRuns };
}

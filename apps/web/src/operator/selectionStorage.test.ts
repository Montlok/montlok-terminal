import { beforeEach, describe, expect, it, vi } from 'vitest';
import {
  readSelection,
  saveSelection,
  selectionKey,
  validateSelection,
} from './selectionStorage';

const { api } = vi.hoisted(() => ({ api: vi.fn() }));
vi.mock('./api', () => ({ api }));

describe('operator-scoped navigation restore', () => {
  beforeEach(() => {
    localStorage.clear();
    vi.clearAllMocks();
  });
  it('migrates an old sandbox selection to the published live Alpha without starting it', async () => {
    api.mockResolvedValue({
      groups: [
        { id: 'live-hft-inventory', mode: 'live' },
        { id: 'live-sector-6040', mode: 'live' },
      ],
    });
    expect(
      await validateSelection({
        selectedGroup: 'baseline',
        selectedRuns: { baseline: 'old-paper' },
      }),
    ).toEqual({ selectedGroup: 'live-sector-6040', selectedRuns: {} });
    expect(api).toHaveBeenCalledExactlyOnceWith('strategy-groups');
  });
  it('retains the live default when the live registry is temporarily unavailable', async () => {
    api.mockResolvedValue({ groups: [] });
    expect(
      await validateSelection({
        selectedGroup: 'baseline',
        selectedRuns: { baseline: 'old' },
      }),
    ).toEqual({ selectedGroup: 'live-sector-6040', selectedRuns: {} });
  });
  it('isolates operator identities and persists navigation only', () => {
    saveSelection('alice', {
      selectedGroup: 'baseline',
      selectedRuns: { baseline: 'run-a' },
    });
    expect(readSelection('bob')).toBeUndefined();
    expect(readSelection('alice')).toEqual({
      selectedGroup: 'baseline',
      selectedRuns: { baseline: 'run-a' },
    });
    expect(
      Object.keys(
        JSON.parse(localStorage.getItem(selectionKey('alice')) || '{}'),
      ),
    ).toEqual(['selectedGroup', 'selectedRuns']);
  });
  it('does not restore malformed values or confirmation fields', () => {
    localStorage.setItem(
      selectionKey('alice'),
      JSON.stringify({
        selectedGroup: 'baseline',
        selectedRuns: { baseline: { id: 'ticket' } },
        ticket: { id: 'secret' },
      }),
    );
    expect(readSelection('alice')).toEqual({
      selectedGroup: 'baseline',
      selectedRuns: {},
    });
    localStorage.setItem(selectionKey('alice'), '{broken');
    expect(readSelection('alice')).toBeUndefined();
  });
  it('requeries groups and run membership and drops deleted or foreign selections', async () => {
    api.mockImplementation(async (path: string) =>
      path === 'strategy-groups'
        ? { groups: [{ id: 'baseline' }, { id: 'enhanced' }] }
        : {
            available: true,
            groups: [
              {
                groupId: path.split('/')[1],
                runs: [{ groupId: 'baseline', runId: 'run-a' }],
              },
            ],
          },
    );
    expect(
      await validateSelection({
        selectedGroup: 'deleted',
        selectedRuns: { baseline: 'run-a', enhanced: 'run-a', deleted: 'old' },
      }),
    ).toEqual({
      selectedGroup: 'baseline',
      selectedRuns: { baseline: 'run-a' },
    });
    expect(api.mock.calls.map(([path]) => path)).toEqual([
      'strategy-groups',
      'strategy-groups/baseline/runtime',
      'strategy-groups/enhanced/runtime',
    ]);
  });
  it('does not restore an action target if runtime verification fails', async () => {
    api
      .mockResolvedValueOnce({ groups: [{ id: 'baseline' }] })
      .mockRejectedValueOnce(new Error('offline'));
    expect(
      await validateSelection({
        selectedGroup: 'baseline',
        selectedRuns: { baseline: 'old' },
      }),
    ).toEqual({ selectedGroup: 'baseline', selectedRuns: {} });
  });
});

import { beforeEach, describe, expect, it, vi } from 'vitest';
import {
  fetchObservation,
  observationClient,
  validateObservation,
  type RunObservation,
} from './runObservation';
import { api } from './api';
vi.mock('./api', () => ({ api: vi.fn() }));
const snapshot = {
  id: 'g',
  runId: 'r',
  accountId: 'account',
  mode: 'live',
  status: 'running',
  metrics: { nav: '123.456789123456789' },
} as RunObservation;
beforeEach(() => {
  vi.mocked(api).mockReset();
  observationClient.clear();
});
describe('shared run observation', () => {
  it('keeps exact financial values and binds the requested run', () => {
    expect(validateObservation(snapshot, 'g', 'r').metrics.nav).toBe(
      '123.456789123456789',
    );
    expect(() =>
      validateObservation({ ...snapshot, runId: 'old' }, 'g', 'r'),
    ).toThrow();
  });
  it('uses only the same live v1 scope when a v2 route is absent', async () => {
    vi.mocked(api)
      .mockRejectedValueOnce(
        Object.assign(new Error('missing'), { status: 404 }),
      )
      .mockResolvedValueOnce(snapshot);
    expect((await fetchObservation('g', 'r')).runId).toBe('r');
    expect(api).toHaveBeenLastCalledWith(
      'strategy-groups/g?view=market&runId=r',
    );
  });
  it('does not conceal permission or consistency errors with a fallback', async () => {
    vi.mocked(api).mockRejectedValue(
      Object.assign(new Error('changed'), { status: 409 }),
    );
    await expect(fetchObservation('g', 'r')).rejects.toThrow('changed');
    expect(api).toHaveBeenCalledTimes(1);
  });
  it('coalesces concurrent consumers of the same run', async () => {
    vi.mocked(api).mockResolvedValue(snapshot);
    const options = {
      queryKey: ['run-observation', 'g', 'r'],
      queryFn: () => fetchObservation('g', 'r'),
      staleTime: 1000,
    };
    await Promise.all([
      observationClient.fetchQuery(options),
      observationClient.fetchQuery(options),
      observationClient.fetchQuery(options),
    ]);
    expect(api).toHaveBeenCalledTimes(1);
  });
});

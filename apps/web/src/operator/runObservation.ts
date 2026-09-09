import { QueryClient, useQuery } from '@tanstack/react-query';
import { api, type Row } from './api';

export type RunObservation = Row & {
  id: string;
  runId: string | null;
  accountId: string | null;
  mode: 'live';
  status: string;
  name?: string;
  metrics: Record<string, string | number | null>;
  observedAt?: number | null;
  fresh?: boolean;
  sources?: Record<string, boolean>;
  provenance?: {
    source: string;
    accounting_basis: string;
    calculation_version: string;
  };
};
export const observationClient = new QueryClient({
  defaultOptions: {
    queries: { retry: 1, refetchOnWindowFocus: true, gcTime: 60_000 },
  },
});
if (typeof window !== 'undefined')
  window.addEventListener(
    'montlok:refresh',
    () =>
      void observationClient.invalidateQueries({
        queryKey: ['run-observation'],
      }),
  );

export function validateObservation(
  value: RunObservation,
  group: string,
  run: string,
) {
  if (
    !value ||
    !value.metrics ||
    typeof value.metrics !== 'object' ||
    Array.isArray(value.metrics)
  )
    throw new Error('运行指标快照格式不完整');
  if (
    value.id !== group ||
    value.mode !== 'live' ||
    (run && value.runId !== run)
  )
    throw new Error('运行数据与所选账户上下文不一致');
  return value;
}
export async function fetchObservation(
  group: string,
  run: string,
): Promise<RunObservation> {
  let value: RunObservation;
  try {
    value = await api<RunObservation>(
      `v2/strategy-groups/${encodeURIComponent(group)}/observation?include=universe,positions,execution,model${run ? `&run_id=${encodeURIComponent(run)}` : ''}`,
    );
  } catch (reason) {
    // A missing v2 route during rolling deployment can use the same live v1 run.
    // Permission errors, stale snapshots and identity conflicts are not retried via v1.
    if ((reason as { status?: number }).status !== 404) throw reason;
    value = await api<RunObservation>(
      `strategy-groups/${encodeURIComponent(group)}?view=market${run ? `&runId=${encodeURIComponent(run)}` : ''}`,
    );
    value = {
      ...value,
      provenance: {
        source: 'operator.live_snapshot',
        accounting_basis: 'reported_run_mark_to_market',
        calculation_version: 'legacy-view-v1',
      },
    };
  }
  return validateObservation(value, group, run);
}
/** One shared read snapshot per group/run, independent from every write/confirmation state. */
export function useRunObservation(group: string, run = '', enabled = true) {
  return useQuery(
    {
      queryKey: ['run-observation', group, run],
      queryFn: () => fetchObservation(group, run),
      enabled: !!group && enabled,
      staleTime: 750,
      refetchInterval: 1000,
      refetchIntervalInBackground: false,
    },
    observationClient,
  );
}

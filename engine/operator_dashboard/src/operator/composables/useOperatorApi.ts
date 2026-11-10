import { onBeforeUnmount, ref, shallowRef } from 'vue';

import { sampleSnapshot } from '../fixtures';
import type { CommandReceipt, OperatorCommand, OperatorSnapshot } from '../types';

type DataSource = 'live' | 'sample' | 'offline';

const API_ROOT = import.meta.env.VITE_OPERATOR_API_ROOT || '/api/operator/v1';
const ENABLE_SAMPLE_DATA = import.meta.env.VITE_ENABLE_SAMPLE_DATA !== 'false';
const POLL_MS = 1_000;

function isSnapshot(value: unknown): value is OperatorSnapshot {
  if (!value || typeof value !== 'object') return false;
  const candidate = value as Partial<OperatorSnapshot>;
  return (
    candidate.schemaVersion === 1 &&
    typeof candidate.runId === 'string' &&
    Array.isArray(candidate.systems) &&
    Array.isArray(candidate.positions) &&
    Array.isArray(candidate.exceptions)
  );
}

export function useOperatorApi() {
  const snapshot = shallowRef<OperatorSnapshot>(structuredClone(sampleSnapshot));
  const source = ref<DataSource>('sample');
  const connectionError = ref('');
  let pollTimer: number | undefined;

  async function refresh() {
    const abort = new AbortController();
    const timeout = window.setTimeout(() => abort.abort(), 1_500);
    try {
      const response = await fetch(`${API_ROOT}/snapshot`, {
        headers: { Accept: 'application/json' },
        credentials: 'same-origin',
        signal: abort.signal,
      });
      if (!response.ok) throw new Error(`Operator API returned HTTP ${response.status}`);
      const next: unknown = await response.json();
      if (!isSnapshot(next)) throw new Error('Operator API snapshot schema is invalid');
      snapshot.value = next;
      source.value = 'live';
      connectionError.value = '';
    } catch (error) {
      connectionError.value = error instanceof Error ? error.message : 'Operator API unavailable';
      source.value = ENABLE_SAMPLE_DATA ? 'sample' : 'offline';
      if (ENABLE_SAMPLE_DATA) {
        snapshot.value = { ...structuredClone(sampleSnapshot), observedAtNs: Date.now() * 1_000_000 };
      }
    } finally {
      window.clearTimeout(timeout);
    }
  }

  async function sendCommand(command: OperatorCommand): Promise<CommandReceipt> {
    if (source.value !== 'live' || !snapshot.value.readyToTrade) {
      throw new Error('控制 API 未认证或交易状态未就绪；未发送命令');
    }
    const idempotencyKey = crypto.randomUUID();
    const response = await fetch(`${API_ROOT}/commands`, {
      method: 'POST',
      credentials: 'same-origin',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': idempotencyKey,
      },
      body: JSON.stringify({
        schemaVersion: 1,
        idempotencyKey,
        expectedRunId: snapshot.value.runId,
        command,
        scope: { accountId: snapshot.value.accountId },
      }),
    });
    if (!response.ok) throw new Error(`控制命令被拒绝（HTTP ${response.status}）`);
    return (await response.json()) as CommandReceipt;
  }

  function start() {
    void refresh();
    pollTimer = window.setInterval(refresh, POLL_MS);
  }

  function stop() {
    if (pollTimer !== undefined) window.clearInterval(pollTimer);
    pollTimer = undefined;
  }

  onBeforeUnmount(stop);
  return { snapshot, source, connectionError, refresh, sendCommand, start, stop };
}

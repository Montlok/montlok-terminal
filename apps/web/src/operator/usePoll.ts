import { useEffect } from 'react';

/**
 * Run `task` immediately and every `intervalMs` while the tab is visible.
 * Hidden tabs pause; returning to the tab refreshes at once. Overlapping runs
 * are skipped. `task` must be referentially stable (wrap it in useCallback).
 */
export function usePoll(
  task: (signal: AbortSignal) => Promise<unknown> | unknown,
  intervalMs: number,
  enabled = true,
): void {
  useEffect(() => {
    if (!enabled) return;
    const controller = new AbortController();
    let inFlight = false;
    let queued = false;
    const run = async (force = false) => {
      if (controller.signal.aborted || document.visibilityState === 'hidden') return;
      if (inFlight) { queued ||= force; return; }
      inFlight = true;
      try {
        await task(controller.signal);
      } finally {
        inFlight = false;
        if (queued && !controller.signal.aborted) { queued = false; void run(); }
      }
    };
    void run();
    const timer = window.setInterval(run, intervalMs);
    const onVisible = () => {
      if (document.visibilityState === 'visible') void run(true);
    };
    document.addEventListener('visibilitychange', onVisible);
    const onRefresh = () => void run(true);
    window.addEventListener('montlok:refresh', onRefresh);
    return () => {
      controller.abort();
      window.clearInterval(timer);
      document.removeEventListener('visibilitychange', onVisible);
      window.removeEventListener('montlok:refresh', onRefresh);
    };
  }, [task, intervalMs, enabled]);
}

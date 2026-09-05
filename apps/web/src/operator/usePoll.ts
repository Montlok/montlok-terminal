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
    const run = async () => {
      if (inFlight || document.visibilityState === 'hidden') return;
      inFlight = true;
      try {
        await task(controller.signal);
      } finally {
        inFlight = false;
      }
    };
    void run();
    const timer = window.setInterval(run, intervalMs);
    const onVisible = () => {
      if (document.visibilityState === 'visible') void run();
    };
    document.addEventListener('visibilitychange', onVisible);
    return () => {
      controller.abort();
      window.clearInterval(timer);
      document.removeEventListener('visibilitychange', onVisible);
    };
  }, [task, intervalMs, enabled]);
}

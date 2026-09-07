import { act, cleanup, render } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { usePoll } from './usePoll';

afterEach(() => { cleanup(); vi.useRealTimers(); });

it('refreshes immediately after an operation and queues one refresh behind an existing read', async () => {
  let complete: (() => void) | undefined;
  const task = vi.fn().mockImplementationOnce(() => new Promise<void>((resolve) => { complete = resolve; })).mockResolvedValue(undefined);
  function Probe() { usePoll(task, 60000); return null; }
  render(<Probe />);
  expect(task).toHaveBeenCalledTimes(1);
  act(() => { window.dispatchEvent(new Event('montlok:refresh')); window.dispatchEvent(new Event('montlok:refresh')); });
  expect(task).toHaveBeenCalledTimes(1);
  await act(async () => complete?.());
  expect(task).toHaveBeenCalledTimes(2);
  await act(async () => window.dispatchEvent(new Event('montlok:refresh')));
  expect(task).toHaveBeenCalledTimes(3);
});

it('removes refresh listeners when the page unmounts', async () => {
  const task=vi.fn().mockResolvedValue(undefined);
  function Probe() { usePoll(task,60000); return null; }
  const view=render(<Probe />);
  await act(async () => {});
  view.unmount();
  window.dispatchEvent(new Event('montlok:refresh'));
  expect(task).toHaveBeenCalledTimes(1);
});

it('keeps ordinary slow polling non-overlapping without queuing every interval', async () => {
  vi.useFakeTimers();
  let complete: (() => void) | undefined;
  const task=vi.fn().mockImplementationOnce(() => new Promise<void>((resolve) => { complete=resolve; })).mockResolvedValue(undefined);
  function Probe() { usePoll(task,1000); return null; }
  render(<Probe />);
  await act(async () => vi.advanceTimersByTime(3000));
  await act(async () => complete?.());
  expect(task).toHaveBeenCalledTimes(1);
});

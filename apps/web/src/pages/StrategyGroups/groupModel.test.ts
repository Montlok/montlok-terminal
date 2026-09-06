import { describe, expect, it } from 'vitest';
import {
  matchingGroup,
  runDateTime,
  runElapsed,
  runRecordCount,
  stateWarning,
  statusLabel,
} from './groupModel';

describe('strategy-group identity', () => {
  it('does not reuse the previous group detail or equity on selection change', () => {
    const baseline = { id: 'baseline', metrics: { nav: 2003 } };
    expect(matchingGroup(baseline, 'enhanced')).toBeUndefined();
    expect(matchingGroup(baseline, 'baseline')).toBe(baseline);
    expect(matchingGroup({ groupId: 'baseline' }, 'enhanced')).toBeUndefined();
    expect(matchingGroup(undefined, 'baseline')).toBeUndefined();
  });

  it('distinguishes pending artifacts, missing data, and completed runs', () => {
    expect(statusLabel('pending_validation')).toBe('待验证');
    expect(statusLabel('unavailable')).toBe('无运行数据');
    expect(statusLabel('completed')).toBe('已结束');
    expect(statusLabel('made-up')).toBe('未知状态');
    expect(statusLabel('halted')).toBe('已暂停');
    expect(statusLabel('stopped')).toBe('已停止');
    expect(stateWarning('halted')?.type).toBe('info');
    expect(statusLabel('failed')).toBe('运行失败');
    expect(stateWarning('error')?.type).toBe('error');
    expect(stateWarning('recovering')).toBeDefined();
    expect(stateWarning('reducing')).toBeDefined();
    expect(stateWarning('unknown')).toBeDefined();
    expect(stateWarning('stopped')).toBeUndefined();
  });
  it('formats observed run times in UTC+8 without inventing missing timestamps', () => {
    expect(runDateTime(Date.parse('2026-09-05T23:00:00Z') / 1000)).toBe(
      '2026/09/06 07:00:00',
    );
    expect(runDateTime(null)).toBe('未记录');
    expect(runDateTime(Number.NaN)).toBe('未记录');
    expect(runElapsed(90061)).toBe('25h 1m');
    expect(runElapsed(null)).toBe('未记录');
    expect(runElapsed(-4)).toBe('未记录');
  });
  it('distinguishes missing execution projections from observed zero records', () => {
    expect(
      runRecordCount({ sources: { orders: false }, orders: [] }, 'orders'),
    ).toBe('未知');
    expect(
      runRecordCount(
        { sources: { fills: false }, fills: [], fillsTotal: 0 },
        'fills',
      ),
    ).toBe('未知');
    expect(
      runRecordCount({ sources: { view: false }, positions: [] }, 'positions'),
    ).toBe('未知');
    expect(
      runRecordCount(
        { sources: { orders: true }, orders: [], ordersTotal: 0 },
        'orders',
      ),
    ).toBe(0);
    expect(
      runRecordCount(
        { fills: [{ id: 'one-shown' }], fillsTotal: 512 },
        'fills',
      ),
    ).toBe(512);
  });
});

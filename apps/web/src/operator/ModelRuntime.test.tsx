import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import { ModelRuntime } from './ModelRuntime';

describe('truthful model runtime facts', () => {
  afterEach(cleanup);
  it('states missing data plainly', () => {
    render(<ModelRuntime />);
    expect(
      screen.getByText('选择模型策略组后显示推理指标'),
    ).toBeInTheDocument();
    expect(screen.queryByText(/不能|不代表/)).not.toBeInTheDocument();
  });
  it('reports a single observation without drawing an interpolated history', () => {
    render(
      <ModelRuntime
        model={{
          recentInferences: [
            {
              completedAtNs: '1788690000000000000',
              prediction: 0.01,
              targetFraction: 0.1,
            },
          ],
        }}
      />,
    );
    expect(screen.getByText('已记录 1 次预测')).toBeInTheDocument();
    expect(screen.getByText('已记录 1 次目标比例')).toBeInTheDocument();
    expect(screen.queryByRole('img')).not.toBeInTheDocument();
  });
  it('does not replace missing inference evidence with zero latency', () => {
    render(
      <ModelRuntime
        model={{
          releaseId: 'release-a',
          modelVersion: 'v1',
          device: 'cpu',
          windowReady: false,
        }}
      />,
    );
    expect(screen.getByText('等待首次推理')).toBeInTheDocument();
    expect(screen.getByText('特征窗口构建中')).toBeInTheDocument();
    expect(screen.queryByText('0 ms')).not.toBeInTheDocument();
    expect(screen.getByText('cpu')).toBeInTheDocument();
  });
  it('keeps genuine zero latency and budget observations', () => {
    render(
      <ModelRuntime
        model={{
          latencyMs: 0,
          queueMs: 1.25,
          budgetWindow: { usedMs: 2, limitMs: 100 },
        }}
      />,
    );
    expect(screen.getByText('0 ms')).toBeInTheDocument();
    expect(screen.getByText('1.25 ms')).toBeInTheDocument();
    expect(screen.getByText('2 / 100 ms')).toBeInTheDocument();
  });
  it('uses final worker status names without calling an unready model loaded', () => {
    render(
      <ModelRuntime
        model={{
          device: 'cpu',
          warmupComplete: false,
          status: 'waiting_for_features',
        }}
      />,
    );
    expect(screen.getByText('cpu（预热中）')).toBeInTheDocument();
    expect(screen.getByText('等待特征窗口')).toBeInTheDocument();
    expect(screen.queryByText('0 ms')).not.toBeInTheDocument();
  });
});

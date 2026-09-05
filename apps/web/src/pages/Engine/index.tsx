import { ReloadOutlined, StopOutlined } from '@ant-design/icons';
import { history, useLocation, useModel } from '@umijs/max';
import { Alert, Button, Tabs } from 'antd';
import { useState } from 'react';
import { api, number, type Row } from '../../operator/api';
import { ConfirmOperation } from '../../operator/ConfirmOperation';
import { DataGrid } from '../../operator/DataGrid';
import { valueLabel } from '../../operator/labels';
import { ResultView } from '../../operator/ResultView';

export default function Engine() {
  const { paper, refresh } = useModel('operator');
  const { pathname } = useLocation();
  const view = pathname.split('/').at(-1) || 'overview';
  const [ticket, setTicket] = useState<Row>();
  const [error, setError] = useState('');
  async function stop() {
    try {
      setTicket(
        await api('prepare', {
          kind: 'native',
          name: 'stop',
          arguments: { runId: paper?.runId },
        }),
      );
    } catch (reason) {
      setError(String(reason));
    }
  }
  return (
    <div className="management-page">
      <div className="page-heading">
        <div>
          <h1>运行概况</h1>
          <p>{paper?.runId || '—'} · 本地模拟</p>
        </div>
        <div className="native-actions">
          <Button icon={<ReloadOutlined />} onClick={() => void refresh()}>
            刷新
          </Button>
          <Button
            danger
            icon={<StopOutlined />}
            disabled={paper?.tradingState !== 'ACTIVE'}
            onClick={() => void stop()}
          >
            停止运行
          </Button>
        </div>
      </div>
      {error && <Alert type="error" title={error} />}
      <div className="engine-summary panel">
        <div>
          <span>运行状态</span>
          <strong
            className={paper?.tradingState === 'ACTIVE' ? 'positive' : ''}
          >
            {valueLabel(paper?.tradingState)}
          </strong>
        </div>
        <div>
          <span>净值</span>
          <strong>{number(paper?.risk?.nav)} USDT</strong>
        </div>
        <div>
          <span>盈亏</span>
          <strong>{number(paper?.risk?.dailyPnl)} USDT</strong>
        </div>
        <div>
          <span>最大回撤</span>
          <strong>{number(Number(paper?.risk?.maxDrawdown) * 100, 3)}%</strong>
        </div>
        <div>
          <span>成交</span>
          <strong>{paper?.risk?.fills ?? '—'}</strong>
        </div>
      </div>
      <Tabs
        activeKey={view === 'overview' ? 'positions' : view}
        onChange={(key) => history.push(`/engine/run/${key}`)}
        destroyOnHidden
        items={[
          {
            key: 'positions',
            label: '持仓',
            children: (
              <DataGrid
                height={450}
                rows={paper?.positions || []}
                columns={[
                  { key: 'instrument', title: '品种', width: 220 },
                  { key: 'quantity', title: '数量' },
                  {
                    key: 'averagePrice',
                    title: '均价',
                    render: (value) => number(value),
                  },
                  {
                    key: 'markPrice',
                    title: '估值',
                    render: (value) => number(value),
                  },
                  {
                    key: 'unrealizedPnl',
                    title: '未实现盈亏',
                    render: (value) => number(value),
                  },
                ]}
              />
            ),
          },
          {
            key: 'strategies',
            label: '策略',
            children: (
              <DataGrid
                rows={paper?.strategies || []}
                columns={[
                  { key: 'id', title: '板块', width: 220 },
                  { key: 'configVersion', title: '配置', width: 220 },
                  { key: 'instrument', title: '品种与过滤条件', width: 300 },
                  {
                    key: 'pnl',
                    title: 'PnL',
                    render: (value) => number(value),
                  },
                  { key: 'mode', title: '状态' },
                ]}
              />
            ),
          },
          {
            key: 'config',
            label: '参数',
            children: (
              <DataGrid
                rows={paper?.runtimeConfig || []}
                height={470}
                columns={[
                  { key: 'key', title: '配置项', width: 250 },
                  { key: 'value', title: '值', width: 740 },
                ]}
              />
            ),
          },
          {
            key: 'health',
            label: '监控',
            children: (
              <>
                <DataGrid
                  rows={paper?.systems || []}
                  columns={[
                    { key: 'label', title: '模块' },
                    { key: 'level', title: '状态' },
                    { key: 'detail', title: '详情', width: 650 },
                  ]}
                />
                <DataGrid
                  rows={paper?.exceptions || []}
                  columns={[
                    { key: 'severity', title: '级别' },
                    { key: 'title', title: '异常', width: 300 },
                    { key: 'detail', title: '详情', width: 600 },
                  ]}
                />
              </>
            ),
          },
          {
            key: 'orders',
            label: '委托',
            children: <ResultView value={paper?.orders || []} />,
          },
          {
            key: 'fills',
            label: '成交',
            children: <ResultView value={paper?.fills || []} />,
          },
        ]}
      />
      <ConfirmOperation
        ticket={ticket}
        onClose={() => setTicket(undefined)}
        onComplete={(value) => {
          setError(
            value.status === 'completed' ? '' : JSON.stringify(value.result),
          );
          void refresh();
        }}
      />
    </div>
  );
}

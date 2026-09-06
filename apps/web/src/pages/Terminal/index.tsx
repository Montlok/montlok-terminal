import { useModel } from '@umijs/max';
import { Button, Tabs } from 'antd';
import { useMemo, useState } from 'react';
import { api, number, type Row, timeOf } from '../../operator/api';
import { ConfirmOperation } from '../../operator/ConfirmOperation';
import { DataGrid, type GridColumn } from '../../operator/DataGrid';
import { valueLabel } from '../../operator/labels';

// Static column sets live at module scope so the memoized grids never see new props per tick.
const FILL_COLUMNS: GridColumn[] = [
  { key: 'fillTime', title: '时间', width: 105, render: timeOf },
  { key: 'instId', title: '交易品种' },
  { key: 'side', title: '方向', width: 90 },
  { key: 'fillPx', title: '成交价', render: (value) => number(value, 6) },
  { key: 'fillSz', title: '成交数量' },
  { key: 'fee', title: '手续费' },
  { key: 'feeCcy', title: '费用币种' },
  { key: 'ordId', title: '订单号', width: 215 },
];
const ASSET_COLUMNS: GridColumn[] = [
  { key: 'ccy', title: '币种' },
  {
    key: 'cashBal',
    title: '余额',
    width: 210,
    render: (value) => number(value, 10),
  },
  {
    key: 'availBal',
    title: '可用',
    width: 210,
    render: (value) => number(value, 10),
  },
  { key: 'frozenBal', title: '冻结', render: (value) => number(value, 8) },
  { key: 'upl', title: '浮动收益' },
];
const PAPER_COLUMNS: GridColumn[] = [
  { key: 'instrument', title: '品种', width: 220 },
  { key: 'quantity', title: '数量', render: (value) => number(value, 6) },
  { key: 'averagePrice', title: '均价', render: (value) => number(value) },
  { key: 'markPrice', title: '估值价格', render: (value) => number(value) },
  {
    key: 'unrealizedPnl',
    title: '未实现 PnL',
    render: (value) => (
      <span className={value < 0 ? 'negative' : 'positive'}>
        {number(value)}
      </span>
    ),
  },
];

export default function Terminal() {
  const { account, paper, error } = useModel('operator');
  const { initialState } = useModel('@@initialState');
  const [ticket, setTicket] = useState<Row>();
  const [actionError, setActionError] = useState('');
  const cancelable =
    initialState?.currentUser?.access === 'admin' && account.mode === 'demo';
  const orderColumns = useMemo<GridColumn[]>(
    () => [
      { key: 'instId', title: '交易品种' },
      { key: 'side', title: '方向', width: 90 },
      { key: 'ordType', title: '类型', width: 100, render: valueLabel },
      { key: 'px', title: '价格', render: (value) => number(value, 6) },
      { key: 'sz', title: '委托数量' },
      { key: 'accFillSz', title: '已成交' },
      { key: 'state', title: '状态', render: valueLabel },
      { key: 'ordId', title: '订单号', width: 215 },
      {
        key: 'action',
        title: '操作',
        width: 85,
        render: (_, row) => (
          <Button
            type="text"
            disabled={!cancelable}
            onClick={() =>
              void api('prepare', {
                kind: 'mcp',
                name: 'spot_cancel_order',
                arguments: { instId: row.instId, ordId: row.ordId },
              })
                .then(setTicket)
                .catch((reason) => setActionError(String(reason)))
            }
          >
            撤单
          </Button>
        ),
      },
    ],
    [cancelable],
  );
  return (
    <div className="terminal-page">
      {(error || actionError) && (
        <div className="status-message">{error || actionError}</div>
      )}
      <section className="blotter panel">
        <Tabs
          size="small"
          items={[
            {
              key: 'orders',
              label: `当前委托 (${account.orders.length})`,
              children: (
                <DataGrid rows={account.orders} columns={orderColumns} />
              ),
            },
            {
              key: 'fills',
              label: `成交 (${account.fills.length})`,
              children: (
                <DataGrid rows={account.fills} columns={FILL_COLUMNS} />
              ),
            },
            {
              key: 'assets',
              label: '资产',
              children: (
                <DataGrid rows={account.balances} columns={ASSET_COLUMNS} />
              ),
            },
            {
              key: 'paper',
              label: '本地模拟',
              children: (
                <>
                  <div className="paper-context">
                    {paper?.runId} · 净值 {number(paper?.risk?.nav)} USDT
                  </div>
                  <DataGrid
                    rows={paper?.positions || []}
                    columns={PAPER_COLUMNS}
                  />
                </>
              ),
            },
          ]}
        />
      </section>
      <footer className="terminal-status">
        <span>账户成交与资产</span>
        <span>
          账户更新 {timeOf(account.updatedAt)} ·{' '}
          {account.mode === 'demo' ? '模拟盘' : '实盘 · 只读'}
        </span>
      </footer>
      <ConfirmOperation
        ticket={ticket}
        onClose={() => setTicket(undefined)}
        onComplete={() => setTicket(undefined)}
      />
    </div>
  );
}

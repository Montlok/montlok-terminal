import { ReloadOutlined } from '@ant-design/icons';
import { ProTable } from '@ant-design/pro-components';
import { useModel } from '@umijs/max';
import { Button } from 'antd';
import { number, type Row, timeOf } from '../../operator/api';
import { DataGrid } from '../../operator/DataGrid';

export default function Account() {
  const { account, refresh } = useModel('operator');
  return (
    <div className="management-page">
      <div className="page-heading">
        <div>
          <h1>资产总览</h1>
          <p>更新于 {timeOf(account.updatedAt)}</p>
        </div>
        <Button icon={<ReloadOutlined />} onClick={() => void refresh()}>
          刷新
        </Button>
      </div>
      <ProTable<Row>
        rowKey="ccy"
        search={false}
        options={false}
        pagination={false}
        size="small"
        dataSource={account.balances}
        className="panel"
        columns={[
          { title: '币种', dataIndex: 'ccy' },
          {
            title: '余额',
            dataIndex: 'cashBal',
            render: (_, row) => number(row.cashBal, 10),
          },
          {
            title: '可用',
            dataIndex: 'availBal',
            render: (_, row) => number(row.availBal, 10),
          },
          {
            title: '冻结',
            dataIndex: 'frozenBal',
            render: (_, row) => number(row.frozenBal, 8),
          },
          {
            title: '估算权益 / USD',
            dataIndex: 'eqUsd',
            render: (_, row) => number(row.eqUsd),
          },
        ]}
      />
      <section className="panel spaced-panel">
        <div className="panel-heading">
          <strong>最近成交</strong>
        </div>
        <DataGrid
          rows={account.fills}
          height={430}
          columns={[
            { key: 'fillTime', title: '时间', render: timeOf },
            { key: 'instId', title: '品种' },
            { key: 'side', title: '方向', width: 80 },
            {
              key: 'fillPx',
              title: '成交价格',
              render: (value) => number(value, 6),
            },
            { key: 'fillSz', title: '数量' },
            { key: 'fee', title: '手续费' },
            { key: 'feeCcy', title: '币种', width: 80 },
            { key: 'ordId', title: '订单号', width: 230 },
          ]}
        />
      </section>
    </div>
  );
}

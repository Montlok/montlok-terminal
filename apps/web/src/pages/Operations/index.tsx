import { ReloadOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import { useEffect, useState } from 'react';
import { api, type Row, timeOf } from '../../operator/api';
import { DataGrid } from '../../operator/DataGrid';

export default function Operations() {
  const [rows, setRows] = useState<Row[]>([]);
  const [error, setError] = useState('');
  const refresh = () => {
    void api('history')
      .then((value) => {
        setRows(value.operations);
        setError('');
      })
      .catch((reason) => setError(String(reason)));
  };
  useEffect(refresh, []);
  return (
    <div className="management-page">
      <div className="page-heading">
        <div>
          <h1>操作记录</h1>
        </div>
        <Button icon={<ReloadOutlined />} onClick={refresh}>
          刷新
        </Button>
      </div>
      {error && <p className="negative">{error}</p>}
      <DataGrid
        rows={rows}
        height={600}
        columns={[
          { key: 'at', title: '时间', render: timeOf },
          { key: 'profile', title: '连接' },
          { key: 'action', title: '操作', width: 320 },
          { key: 'status', title: '结果' },
          { key: 'parameters', title: '参数', width: 440 },
          { key: 'result', title: '详情', width: 560 },
          { key: 'id', title: '操作 ID', width: 300 },
        ]}
      />
    </div>
  );
}

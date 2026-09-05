import { ExportOutlined, ReloadOutlined } from '@ant-design/icons';
import { Alert, Button, Descriptions, Progress, Space, Tag } from 'antd';
import { useEffect, useState } from 'react';
import { api, number, type Row, timeOf } from '../../operator/api';

export default function Server() {
  const [host, setHost] = useState<Row>();
  const [error, setError] = useState('');
  const refresh = () => void api('host').then(value => {setHost(value);setError('');}).catch(reason=>setError(String(reason)));
  useEffect(() => {refresh(); const timer=setInterval(refresh,5000);return()=>clearInterval(timer);},[]);
  return <div className="management-page">
    <div className="page-heading"><h1>宝塔面板</h1><Space>
      <Button icon={<ReloadOutlined />} onClick={refresh}>刷新</Button>
      <Button type="primary" icon={<ExportOutlined />} href={host?.panelUrl} target="_blank" rel="noreferrer" disabled={!host}>打开面板</Button>
    </Space></div>
    {error && <Alert title={error} type="error" />}
    <section className="panel function-panel">
      <Descriptions column={2} items={[
        {key:'panel',label:'面板端口',children:<Tag color={host?.panelReachable?'green':'default'}>{host?.panelReachable?'连通':'—'}</Tag>},
        {key:'cores',label:'CPU',children:`${host?.cores ?? '—'} 核`},
        {key:'memory',label:'内存',children:`${number(Number(host?.memoryUsed)/2**30)} / ${number(Number(host?.memoryTotal)/2**30)} GiB`},
        {key:'disk',label:'磁盘',children:`${number(Number(host?.diskUsed)/2**30)} / ${number(Number(host?.diskTotal)/2**30)} GiB`},
        {key:'uptime',label:'运行时间',children:`${number(Number(host?.uptime)/86400,1)} 天`},
        {key:'at',label:'更新',children:timeOf(host?.at)},
      ]} />
      <div className="host-meters">{[['cpu','CPU'],['memoryPercent','内存'],['diskPercent','磁盘']].map(([key,label])=><div key={key}><span>{label}</span><Progress percent={host?.[key] ?? 0} size="small" /></div>)}</div>
    </section>
  </div>;
}

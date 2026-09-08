import {useModel} from '@umijs/max';
import {useEffect,useMemo,useRef,useState,useSyncExternalStore} from 'react';
import {TerminalClient} from '@montlok/sdk';
import {EventEnvelope,EventType} from '@montlok/sdk/protocol';
import {ensureSession} from './api';
import {DataGrid} from './DataGrid';

const timeFormatter=new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',hour12:false,hour:'2-digit',minute:'2-digit',second:'2-digit'});
function eventTime(ns:bigint){
  if(ns<=0n)return '—';
  const seconds=ns/1_000_000_000n;const fraction=(ns%1_000_000_000n).toString().padStart(9,'0');
  const time=timeFormatter.format(Number(seconds)*1000);
  return `${time}.${fraction}`;
}
const EVENT_LABELS:Partial<Record<EventType,string>>={
  [EventType.ORDER_SUBMITTED]:'委托提交',[EventType.ORDER_ACCEPTED]:'委托确认',[EventType.ORDER_REJECTED]:'委托拒绝',
  [EventType.ORDER_CANCEL_REQUESTED]:'撤单提交',[EventType.ORDER_CANCELED]:'撤单确认',[EventType.FILL_RECEIVED]:'成交',
  [EventType.POSITION_UPDATED]:'持仓更新',[EventType.PNL_UPDATED]:'收益更新',[EventType.RUN_STATE_CHANGED]:'运行状态',
  [EventType.MODEL_INFERENCE_COMPLETED]:'模型推理',[EventType.SIGNAL_GENERATED]:'信号',[EventType.RISK_DECISION]:'风控',
  [EventType.TARGET_POSITION_CHANGED]:'目标更新',[EventType.ROUTE_UPDATED]:'路由更新',[EventType.ALERT_RAISED]:'告警',
  [EventType.ALERT_CLEARED]:'告警恢复',[EventType.OPERATION_RECEIPT_UPDATED]:'操作回执',
};
const PHASE={connecting:'连接中',snapshot:'同步快照',live:'已同步',recovering:'恢复事件',disconnected:'连接中断'};
type BlotterRow={time:string;instrument:string;type:string;sequence:string;correlation:string;source:string;event:EventEnvelope};

export default function EventBlotter(){
  const {selectedGroup,selectedRuns}=useModel('operator');const runId=selectedRuns?.[selectedGroup]||'';
  const [error,setError]=useState('');const [selected,setSelected]=useState<EventEnvelope>();
  const [client]=useState(()=>{
    const url=new URL('/api/v2/stream',window.location.href);url.protocol=location.protocol==='https:'?'wss:':'ws:';
    return new TerminalClient(url.toString(),setError);
  });
  const state=useSyncExternalStore(client.store.subscribe,client.store.getSnapshot,client.store.getSnapshot);
  const rendered=useRef(new Map<string,BlotterRow>());
  useEffect(()=>{
    let active=true;setSelected(undefined);setError('');
    if(runId)void ensureSession().then(()=>{if(active)client.start({strategyGroupId:selectedGroup,runId},[`run.${runId}`]);}).catch(reason=>setError(String(reason)));
    return()=>{active=false;client.stop();};
  },[client,selectedGroup,runId]);
  const rows=useMemo(()=>{
    const next=new Map<string,BlotterRow>();
    for(const [key,event] of state.events){if(event.runId!==runId)continue;
      const prior=rendered.current.get(key);
      next.set(key,prior?.event===event?prior:{time:eventTime(event.occurredAtNs),instrument:event.instrumentId,type:EVENT_LABELS[event.eventType]||EventType[event.eventType],
        sequence:event.streamSeq.toString(),correlation:event.correlationId||'—',source:event.source,event});
    }
    rendered.current=next;return [...next.values()].sort((a,b)=>a.event.streamSeq>b.event.streamSeq?-1:a.event.streamSeq<b.event.streamSeq?1:0);
  },[state.version,runId]);
  const details=selected?JSON.stringify(EventEnvelope.toJSON(selected),null,2):'';
  return <section className="event-blotter" style={{height:'100%',display:'flex',flexDirection:'column',overflow:'auto'}}>
    <div className="panel-heading"><strong>运行事件</strong><span>{PHASE[state.phase]} · {rows.length} 条 · 合并 {state.conflated}</span></div>
    <p className="execution-note">{runId||'选择运行实例'}{error?` · ${error}`:''}</p>
    <DataGrid rows={rows} height={360} emptyLabel={runId?'等待运行事件':'选择运行实例后读取事件'} onSelectRow={row=>setSelected(row.event)} columns={[
      {key:'time',title:'时间 / UTC+8',width:180},{key:'instrument',title:'品种',width:160},{key:'type',title:'事件',width:100},
      {key:'sequence',title:'序号',width:95},{key:'correlation',title:'关联编号',width:245},{key:'source',title:'来源',width:160},
    ]}/>
    {selected&&<section aria-label="事件详情" style={{padding:12,borderTop:'1px solid var(--op-border)'}}><strong>事件详情 · {selected.eventId}</strong>
      <pre style={{fontSize:12,whiteSpace:'pre-wrap',overflowWrap:'anywhere'}}>{details}</pre></section>}
  </section>;
}

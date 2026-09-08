import { tableFromIPC } from 'apache-arrow';
import { EventEnvelope, EventType, type ServerMessage, type ResumePosition } from './generated/montlok/v2/terminal';

export type StreamPhase='connecting'|'snapshot'|'live'|'recovering'|'disconnected';
export type StreamState={
  version:number;phase:StreamPhase;events:ReadonlyMap<string,EventEnvelope>;
  positions:ReadonlyMap<string,bigint>;conflated:number;gaps:number;lastHeartbeatNs:bigint;
};
export type Recovery={stream:string;after:bigint};
type StagedSnapshot={events:Map<string,EventEnvelope>;positions:Map<string,bigint>};

/** Critical events are delivered individually; view notifications render at most 20 Hz. */
export class TerminalStreamStore {
  private working:StreamState={version:0,phase:'connecting',events:new Map(),positions:new Map(),conflated:0,gaps:0,lastHeartbeatNs:0n};
  private state:StreamState={...this.working,events:new Map(),positions:new Map()};
  private listeners=new Set<()=>void>();
  private criticalListeners=new Set<(event:EventEnvelope)=>void>();
  private snapshots=new Map<string,StagedSnapshot>();
  private notifyTimer:ReturnType<typeof setTimeout>|undefined;
  private pendingMarket=new Map<string,EventEnvelope>();
  private notifyQueued=false;

  constructor(private readonly recover:(request:Recovery)=>void,private readonly frameMs=50) {}
  getSnapshot=():StreamState=>this.state;
  subscribe=(listener:()=>void)=>{this.listeners.add(listener);return()=>{this.listeners.delete(listener);};};
  onCritical=(listener:(event:EventEnvelope)=>void)=>{this.criticalListeners.add(listener);return()=>{this.criticalListeners.delete(listener);};};
  resumePositions():ResumePosition[]{return [...this.working.positions].map(([stream,lastStreamSeq])=>({stream,lastStreamSeq}));}
  disconnected(){
    this.snapshots.clear();
    const events=this.working.events as Map<string,EventEnvelope>;
    for(const [key,event] of this.pendingMarket)events.set(key,event);
    this.pendingMarket.clear();this.update({events,phase:'disconnected'});
  }
  connecting(){this.update({phase:'connecting'});}
  dispose(){if(this.notifyTimer)clearTimeout(this.notifyTimer);this.listeners.clear();this.criticalListeners.clear();this.snapshots.clear();this.pendingMarket.clear();}

  ingest(message:ServerMessage):void {
    switch(message.message?.$case){
      case 'snapshotBegin':{
        const begin=message.message.snapshotBegin;
        const positions=new Map(begin.watermarks.map(position=>[position.stream,position.lastStreamSeq]));
        for(const [stream,sequence] of positions)if(sequence<(this.working.positions.get(stream)??0n))throw new Error('陈旧快照序号');
        this.snapshots.set(begin.subscriptionId,{events:new Map(),positions});
        this.update({phase:'snapshot'});break;
      }
      case 'snapshotBatch':{
        const batch=message.message.snapshotBatch;const staged=this.snapshots.get(batch.subscriptionId);
        if(!staged)throw new Error('快照批次缺少开始消息');
        if(batch.arrowIpc.byteLength>1_048_576||batch.rowCount>2048)throw new Error('快照批次超过协议容量');
        const table=tableFromIPC(batch.arrowIpc);
        if(table.numRows!==batch.rowCount)throw new Error('快照行数不一致');
        const payloads=table.getChild('payload_protobuf');if(!payloads)throw new Error('快照缺少事件列');
        for(let row=0;row<table.numRows;row++){
          const bytes=payloads.get(row);if(!(bytes instanceof Uint8Array))throw new Error('快照事件格式错误');
          const event=EventEnvelope.decode(bytes);
          if(event.streamSeq>(staged.positions.get(event.stream)??0n))throw new Error('事件超过快照水位');
          staged.events.set(entityKey(event),event);
        }
        break;
      }
      case 'snapshotEnd':{
        const end=message.message.snapshotEnd;const staged=this.snapshots.get(end.subscriptionId);
        if(staged){
          const final=new Map(end.watermarks.map(p=>[p.stream,p.lastStreamSeq]));
          if(final.size!==staged.positions.size||[...final].some(([s,n])=>staged.positions.get(s)!==n))throw new Error('快照水位不一致');
          this.snapshots.delete(end.subscriptionId);this.pendingMarket.clear();
          this.update({events:staged.events,positions:staged.positions,phase:'live'});
        }else{
          // Replay completion never advances beyond events actually received.
          if(end.watermarks.some(p=>(this.working.positions.get(p.stream)??0n)<p.lastStreamSeq))throw new Error('回放未达到确认水位');
          this.update({phase:'live'});
        }
        break;
      }
      case 'event':this.accept(message.message.event);break;
      case 'heartbeat':this.update({lastHeartbeatNs:message.message.heartbeat.serverTimeNs});break;
      case 'sequenceGap':{
        const gap=message.message.sequenceGap;
        this.update({phase:'recovering',gaps:this.working.gaps+1});
        this.recover({stream:gap.stream,after:this.working.positions.get(gap.stream)??0n});break;
      }
      case 'error':{
        this.update({phase:'recovering'});
        if(message.message.error.retryable)this.recover({stream:'*',after:0n});
        else throw new Error(message.message.error.detail);
        break;
      }
    }
  }
  private accept(event:EventEnvelope){
    if(event.schemaVersion!==2)throw new Error('事件版本不兼容');
    const previous=this.working.positions.get(event.stream)??0n;
    if(event.streamSeq<=previous)return;
    if(event.streamSeq!==previous+1n){
      this.update({phase:'recovering',gaps:this.working.gaps+1});this.recover({stream:event.stream,after:previous});return;
    }
    const positions=this.working.positions as Map<string,bigint>;positions.set(event.stream,event.streamSeq);
    if(event.eventType===EventType.MARKET_QUOTE||event.eventType===EventType.ORDER_BOOK_UPDATED){
      const key=entityKey(event);const replaced=this.pendingMarket.has(key);
      this.pendingMarket.set(key,event);
      this.update({positions,conflated:this.working.conflated+(replaced?1:0)});
    }else{
      const events=this.working.events as Map<string,EventEnvelope>;events.set(entityKey(event),event);
      // Blotter history is queryable on the server. Keep current view memory bounded.
      if(events.size>20_000){const oldest=events.keys().next().value;if(oldest!==undefined)events.delete(oldest);}
      this.update({events,positions});for(const listener of this.criticalListeners)listener(event);
    }
  }
  private update(patch:Partial<StreamState>){
    this.working={...this.working,...patch};
    if(this.notifyQueued)return;this.notifyQueued=true;
    this.notifyTimer=setTimeout(()=>{
      this.notifyQueued=false;
      if(this.pendingMarket.size){const events=this.working.events as Map<string,EventEnvelope>;for(const [key,event]of this.pendingMarket)events.set(key,event);this.pendingMarket.clear();this.working={...this.working,events};}
      this.working={...this.working,version:this.working.version+1};this.state={...this.working,events:new Map(this.working.events),positions:new Map(this.working.positions)};for(const listener of this.listeners)listener();
    },this.frameMs);
  }
}

export function entityKey(event:EventEnvelope):string{
  const prefix=`${event.stream}/`;
  switch(event.payload?.$case){
    case 'marketQuote':return `${prefix}quote/${event.instrumentId}`;
    case 'orderBookUpdated':return `${prefix}book/${event.instrumentId}`;
    case 'orderEvent':return `${prefix}order/${event.payload.orderEvent.clientOrderId}`;
    case 'routeUpdated':return `${prefix}route/${event.payload.routeUpdated.routeId}`;
    case 'fillReceived':return `${prefix}fill/${event.payload.fillReceived.tradeId}`;
    case 'positionUpdated':return `${prefix}position/${event.instrumentId}`;
    case 'pnlUpdated':return `${prefix}pnl/${event.runId}/${event.instrumentId}`;
    case 'runStateChanged':return `${prefix}state/${event.runId}`;
    case 'alertChanged':return `${prefix}alert/${event.payload.alertChanged.alertId}`;
    case 'operationReceiptUpdated':return `${prefix}operation/${event.payload.operationReceiptUpdated.operationId}`;
    default:return prefix+event.eventId;
  }
}

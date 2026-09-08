import { ClientMessage, ServerMessage, TerminalContext } from './generated/montlok/v2/terminal';
import { TerminalStreamStore } from './streamStore';

export class TerminalClient {
  readonly store:TerminalStreamStore;
  private socket:WebSocket|undefined;
  private retry:ReturnType<typeof setTimeout>|undefined;
  private stopped=true;
  private retries=0;
  private context=TerminalContext.fromPartial({});
  private topics:string[]=['*'];
  private subscriptionId='';
  constructor(private readonly url:string,private readonly reportError:(detail:string)=>void=()=>{}){
    this.store=new TerminalStreamStore(()=>this.reconnect());
  }
  start(context:Partial<TerminalContext>={},topics=['*']){
    this.context=TerminalContext.fromPartial(context);this.topics=topics;this.stopped=false;this.connect();
  }
  setContext(context:Partial<TerminalContext>){
    this.context=TerminalContext.fromPartial(context);
    if(this.socket?.readyState===WebSocket.OPEN)this.subscribe();
  }
  stop(){this.stopped=true;if(this.retry)clearTimeout(this.retry);this.socket?.close();this.store.disconnected();}
  dispose(){this.stop();this.store.dispose();}
  private connect(){
    this.retry=undefined;
    if(this.stopped)return;
    this.store.connecting();const socket=new WebSocket(this.url,'montlok.protobuf.v2');socket.binaryType='arraybuffer';this.socket=socket;
    socket.onopen=()=>{
      if(this.socket!==socket||this.stopped){socket.close();return;}
      this.retries=0;
      this.send({message:{$case:'hello',hello:{protocolVersion:2,deviceId:'web',resumePositions:this.store.resumePositions()}}});
      this.subscribe();
    };
    socket.onmessage=(message)=>{
      if(this.socket!==socket)return;
      try{if(!(message.data instanceof ArrayBuffer))throw new Error('实时消息格式错误');this.store.ingest(ServerMessage.decode(new Uint8Array(message.data)));}
      catch(error){this.reportError(error instanceof Error?error.message:String(error));this.reconnect();}
    };
    socket.onclose=()=>{
      if(this.socket!==socket)return;this.socket=undefined;this.store.disconnected();
      if(!this.stopped){const delay=Math.min(10_000,250*2**this.retries++);this.retry=setTimeout(()=>this.connect(),delay+Math.random()*200);}
    };
    socket.onerror=()=>this.reportError('事件流连接正在恢复');
  }
  private subscribe(){
    this.subscriptionId=crypto.randomUUID();
    this.send({message:{$case:'subscribe',subscribe:{requestId:this.subscriptionId,topics:this.topics,context:this.context,fields:[],conflateMs:50}}});
  }
  private send(message:ClientMessage){if(this.socket?.readyState===WebSocket.OPEN)this.socket.send(ClientMessage.encode(message).finish());}
  private reconnect(){if(this.socket)this.socket.close();else if(!this.stopped&&!this.retry)this.retry=setTimeout(()=>this.connect(),250);}
}

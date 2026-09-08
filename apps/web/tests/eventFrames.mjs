// Local UI contract fixtures only. This file is excluded from runtime builds.
import {EventEnvelope,EventType,ServerMessage,Side,OrderState} from '../../../packages/sdk-ts/src/generated/montlok/v2/terminal.ts';
const context=JSON.parse(process.env.MONTLOK_EVENT_CONTEXT);
const payloads=[
  [EventType.ORDER_SUBMITTED,{$case:'orderEvent',orderEvent:{clientOrderId:'contract-order',quantity:{value:'0.001'},price:{value:'123.456789'},side:Side.BUY,state:OrderState.ORDER_PENDING_SUBMIT}}],
  [EventType.ORDER_ACCEPTED,{$case:'orderEvent',orderEvent:{clientOrderId:'contract-order',venueOrderId:'contract-venue-order',quantity:{value:'0.001'},price:{value:'123.456789'},side:Side.BUY,state:OrderState.ORDER_ACCEPTED_BY_VENUE}}],
  [EventType.FILL_RECEIVED,{$case:'fillReceived',fillReceived:{tradeId:'contract-trade',routeId:'contract-route',venueOrderId:'contract-venue-order',price:{value:'123.456789'},quantity:{value:'0.001'},fee:{value:'0.0001'},feeCurrency:'USDT',side:Side.BUY,liquidity:'maker'}}],
];
const stream=`run.${context.runId}`;
const events=payloads.map(([eventType,payload],index)=>EventEnvelope.fromPartial({...context,schemaVersion:2,stream,streamSeq:BigInt(index+1),eventId:`contract-event-${index+1}`,correlationId:'contract-correlation',
  occurredAtNs:1800000000123456789n+BigInt(index)*10_000_000n,receivedAtNs:1800000000123456799n+BigInt(index)*10_000_000n,eventType,payload,source:'local.ui-contract-fixture'}));
const watermarks=[{stream,lastStreamSeq:0n}];
const messages=[{message:{$case:'snapshotBegin',snapshotBegin:{subscriptionId:'contract',watermarks}}},{message:{$case:'snapshotEnd',snapshotEnd:{subscriptionId:'contract',watermarks}}},...events.map(event=>({message:{$case:'event',event}}))];
console.log(JSON.stringify({frames:messages.map(message=>Buffer.from(ServerMessage.encode(ServerMessage.fromPartial(message)).finish()).toString('base64')),
  related:{encoding:'protobuf-base64',events:events.map(event=>Buffer.from(EventEnvelope.encode(event).finish()).toString('base64')),truncated:false}}));

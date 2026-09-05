import { Checkbox, Select, Segmented } from 'antd';
import { lazy, Suspense, useState } from 'react';
import type { Row } from './api';
import { MarketChart } from './MarketChart';
import { useMarket } from './useMarket';
const DepthChart=lazy(()=>import('./DepthChart'));

function SecondaryChart({instrument,mode}: {instrument:string;mode:string}) {
  const [bar,setBar]=useState('1H');
  const market=useMarket(instrument,bar,mode);
  return <section><div className="chart-secondary-toolbar"><Select aria-label="副图周期" value={bar} onChange={setBar} options={['1m','5m','15m','1H','4H','1D'].map(value=>({value,label:value}))} /></div><MarketChart candles={market.candles} candle={market.candle} /></section>;
}
export function MarketWorkspace({market,instrument,mode}: {market:Row;instrument:string;mode:string}) {
  const [view,setView]=useState('candles');
  const [averages,setAverages]=useState(true);
  return <>
    <div className="chart-view-toolbar">
      <Segmented aria-label="图表视图" value={view} onChange={setView} options={[{value:'candles',label:'K 线'},{value:'dual',label:'双图'},{value:'depth',label:'深度'}]} />
      <Checkbox checked={averages} onChange={event=>setAverages(event.target.checked)}>MA 20 / 60</Checkbox>
    </div>
    {view==='depth' ? <Suspense fallback={<div className="empty-state">加载中</div>}><DepthChart book={market.book} /></Suspense> : <div className={view==='dual'?'chart-double':''}>
      <section><MarketChart candles={market.candles || []} candle={market.candle} averages={averages} /></section>
      {view==='dual' && <SecondaryChart instrument={instrument} mode={mode} />}
    </div>}
  </>;
}

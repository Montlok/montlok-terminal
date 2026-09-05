import type { Row } from './api';
export function movingAverage(candles: Row[], period: number) {
  let sum=0;
  return candles.flatMap((bar,index)=>{
    sum += Number(bar.close);
    if(index>=period) sum-=Number(candles[index-period].close);
    return index+1>=period ? [{time:bar.time,value:sum/period}] : [];
  });
}
export function depthLevels(book: Row) {
  const result: Row[]=[];
  for(const [key,side] of [['bids','买盘'],['asks','卖盘']]) {
    let cumulative=0;
    for(const row of book?.[key] || []) {
      cumulative+=Number(row[1]);
      result.push({price:Number(row[0]),volume:cumulative,side});
    }
  }
  return result.sort((a,b)=>a.price-b.price);
}

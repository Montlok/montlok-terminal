import { Area } from '@ant-design/plots';
import { useMemo } from 'react';
import type { Row } from './api';
import { depthLevels } from './indicators';

export default function DepthChart({book}: {book:Row}) {
  const data=useMemo(()=>depthLevels(book),[book]);
  if(!data.length) return <div className="empty-state">暂无盘口</div>;
  return <Area height={410} data={data} xField="price" yField="volume" colorField="side" theme="classicDark"
    scale={{x:{type:'linear'},color:{domain:['买盘','卖盘'],range:['#25b77d','#f05b65']}} style={{fillOpacity:0.25}}
    axis={{x:{title:'价格 (USDT)'},y:{title:'累计数量'}} animate={false} />;
}

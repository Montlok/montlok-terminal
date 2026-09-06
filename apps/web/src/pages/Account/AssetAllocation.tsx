import { memo, useMemo } from 'react';
import { number, type Row } from '../../operator/api';
import { assetAllocation } from './allocationModel';
import './allocation.css';

const COLORS = [
  '#7fabff',
  '#25b77d',
  '#d3ad59',
  '#9c83cb',
  '#58c6cc',
  '#f08b65',
];
export const AssetAllocation = memo(function AssetAllocation({
  balances,
}: {
  balances: Row[];
}) {
  const allocation = useMemo(() => assetAllocation(balances), [balances]);
  let offset = 0;
  return (
    <section className="panel allocation-panel">
      <div className="panel-heading">
        <strong>资产分布</strong>
        <span>按当前 USD 权益估值</span>
      </div>
      {allocation.slices.length ? (
        <div className="allocation-content">
          <div className="allocation-graphic">
            <svg viewBox="0 0 220 220" role="img" aria-label="正权益资产占比">
              {allocation.slices.map((slice, index) => {
                const start = offset;
                offset += slice.share * 100;
                return (
                  <circle
                    key={slice.currency}
                    cx="110"
                    cy="110"
                    r="86"
                    pathLength="100"
                    fill="none"
                    stroke={COLORS[index % COLORS.length]}
                    strokeWidth="18"
                    strokeDasharray={`${slice.share * 100} ${100 - slice.share * 100}`}
                    strokeDashoffset={-start}
                    transform="rotate(-90 110 110)"
                  >
                    <title>
                      {slice.currency}：{number(slice.value)} USD，
                      {number(slice.share * 100)}%
                    </title>
                  </circle>
                );
              })}
              <text
                x="110"
                y="101"
                textAnchor="middle"
                className="allocation-caption"
              >
                正权益 / USD
              </text>
              <text
                x="110"
                y="127"
                textAnchor="middle"
                className="allocation-total"
              >
                {number(allocation.positive)}
              </text>
            </svg>
          </div>
          <div className="allocation-legend">
            {allocation.slices.map((slice, index) => (
              <div className="allocation-row" key={slice.currency}>
                <span className="allocation-currency">
                  <i
                    style={{ backgroundColor: COLORS[index % COLORS.length] }}
                  />
                  {slice.currency}
                </span>
                <span>{number(slice.value)} USD</span>
                <span>{number(slice.share * 100)}%</span>
              </div>
            ))}
          </div>
        </div>
      ) : (
        <div className="empty-state">暂无资产估值</div>
      )}
      {(allocation.negative < 0 || allocation.missing > 0) && (
        <div className="allocation-note">
          {allocation.negative < 0 && (
            <span>负权益 {number(allocation.negative)} USD</span>
          )}
          {allocation.missing > 0 && (
            <span>{allocation.missing} 项资产尚无 USD 估值</span>
          )}
        </div>
      )}
    </section>
  );
});

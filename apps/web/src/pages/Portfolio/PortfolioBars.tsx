import { memo } from 'react';
import { number } from '../../operator/api';
import { barGeometry, type PortfolioBar } from './portfolioModel';

export const PortfolioBars = memo(function PortfolioBars({
  rows,
  label,
  signed = false,
  available,
  missing = 0,
}: {
  rows: PortfolioBar[];
  label: string;
  signed?: boolean;
  available: boolean;
  missing?: number;
}) {
  const maximum = Math.max(0, ...rows.map((row) => Math.abs(row.value)));
  return (
    <section className="portfolio-bars" aria-label={label}>
      {!available ? (
        <div className="portfolio-empty">持仓数据正在对齐</div>
      ) : !rows.length ? (
        <div className="portfolio-empty">
          {missing ? '估值数据正在补充' : '持仓记录将随成交更新'}
        </div>
      ) : (
        <div className="portfolio-bars-list">
          {rows.map((row) => {
            const geometry = barGeometry(row.value, maximum, signed);
            return (
              <div className="portfolio-bar-row" key={row.label}>
                <span className="portfolio-bar-label" title={row.label}>
                  {row.label.replace('.OKX', '')}
                </span>
                <div
                  className={`portfolio-bar-track ${signed ? 'signed' : ''}`}
                  aria-hidden="true"
                >
                  <i
                    className={
                      signed ? (row.value < 0 ? 'loss' : 'gain') : 'exposure'
                    }
                    style={{
                      left: `${geometry.left}%`,
                      width: `${geometry.width}%`,
                    }}
                  />
                </div>
                <span
                  className={
                    signed ? (row.value < 0 ? 'negative' : 'positive') : ''
                  }
                >
                  {signed && row.value > 0 ? '+' : ''}
                  {number(row.value, 2)}
                </span>
              </div>
            );
          })}
        </div>
      )}
      {missing > 0 && (
        <p className="portfolio-data-note">{missing} 项持仓缺少估值</p>
      )}
    </section>
  );
});

import { useEffect, useMemo, useRef } from 'react';
import { number, type Row } from './api';
import { type DepthLevel, type DepthProfile, depthProfile } from './indicators';

const COLORS = {
  bid: '#25b77d',
  ask: '#f05b65',
  grid: '#20252a',
  text: '#929ba6',
};
const PADDING = { top: 14, right: 12, bottom: 22, left: 12 };

/**
 * Cumulative order-book depth drawn straight onto a canvas.
 *
 * A books5/books20 frame arrives many times a second; repainting forty
 * levels costs well under a millisecond and needs no charting library.
 */
export default function DepthChart({
  book,
  height = 410,
}: {
  book: Row;
  height?: number;
}) {
  const canvas = useRef<HTMLCanvasElement>(null);
  const latest = useRef<DepthProfile>({ bids: [], asks: [] });
  const profile = useMemo(() => depthProfile(book), [book]);
  const hasLevels = profile.bids.length > 0 || profile.asks.length > 0;
  useEffect(() => {
    const element = canvas.current;
    if (!element) return;
    const observer = new ResizeObserver(() => paint(element, latest.current));
    observer.observe(element);
    return () => observer.disconnect();
  }, [hasLevels]);
  useEffect(() => {
    latest.current = profile;
    if (canvas.current) paint(canvas.current, profile);
  }, [profile]);
  if (!hasLevels) return <div className="empty-state">暂无盘口</div>;
  return (
    <canvas
      ref={canvas}
      className="depth-chart"
      style={{ height }}
      role="img"
      aria-label="累计深度图"
    />
  );
}

function paint(element: HTMLCanvasElement, { bids, asks }: DepthProfile) {
  const width = element.clientWidth;
  const height = element.clientHeight;
  if (!width || !height) return;
  const scale = window.devicePixelRatio || 1;
  if (element.width !== Math.round(width * scale))
    element.width = Math.round(width * scale);
  if (element.height !== Math.round(height * scale))
    element.height = Math.round(height * scale);
  const context = element.getContext('2d');
  if (!context) return;
  context.setTransform(scale, 0, 0, scale, 0, 0);
  context.clearRect(0, 0, width, height);

  const low = bids.at(-1)?.price ?? asks[0]?.price ?? 0;
  const high = asks.at(-1)?.price ?? bids[0]?.price ?? 0;
  const maxCumulative =
    Math.max(bids.at(-1)?.cumulative ?? 0, asks.at(-1)?.cumulative ?? 0) || 1;
  const plotWidth = width - PADDING.left - PADDING.right;
  const plotHeight = height - PADDING.top - PADDING.bottom;
  const x = (price: number) =>
    PADDING.left +
    (high === low ? plotWidth / 2 : ((price - low) / (high - low)) * plotWidth);
  const y = (cumulative: number) =>
    PADDING.top + plotHeight - (cumulative / maxCumulative) * plotHeight;

  context.strokeStyle = COLORS.grid;
  context.lineWidth = 1;
  for (let step = 0; step <= 4; step++) {
    const line = Math.round(PADDING.top + (plotHeight * step) / 4) + 0.5;
    context.beginPath();
    context.moveTo(PADDING.left, line);
    context.lineTo(PADDING.left + plotWidth, line);
    context.stroke();
  }

  const side = (levels: DepthLevel[], color: string, edge: number) => {
    if (!levels.length) return;
    const trace = () => {
      context.moveTo(x(levels[0].price), y(0));
      let previous = 0;
      for (const level of levels) {
        context.lineTo(x(level.price), y(previous));
        context.lineTo(x(level.price), y(level.cumulative));
        previous = level.cumulative;
      }
      context.lineTo(edge, y(previous));
    };
    context.beginPath();
    trace();
    context.lineTo(edge, y(0));
    context.closePath();
    context.fillStyle = `${color}40`;
    context.fill();
    context.beginPath();
    trace();
    context.strokeStyle = color;
    context.lineWidth = 1.5;
    context.stroke();
  };
  side(bids, COLORS.bid, PADDING.left);
  side(asks, COLORS.ask, PADDING.left + plotWidth);

  context.fillStyle = COLORS.text;
  context.font =
    '11px -apple-system, BlinkMacSystemFont, "PingFang SC", sans-serif';
  const baseline = height - 7;
  context.textAlign = 'left';
  context.fillText(number(low, 4), PADDING.left, baseline);
  context.fillText(number(maxCumulative, 2), PADDING.left, PADDING.top - 4);
  context.textAlign = 'right';
  context.fillText(number(high, 4), PADDING.left + plotWidth, baseline);
  if (bids.length && asks.length) {
    context.textAlign = 'center';
    context.fillText(
      number((bids[0].price + asks[0].price) / 2, 4),
      x((bids[0].price + asks[0].price) / 2),
      baseline,
    );
  }
}

import { number, type Row } from './api';

export function incrementDigits(increment: unknown): number | undefined {
  if (typeof increment !== 'string' && typeof increment !== 'number')
    return undefined;
  if (!Number.isFinite(Number(increment)) || Number(increment) <= 0)
    return undefined;
  const [mantissa, exponent = '0'] = String(increment).toLowerCase().split('e');
  const fraction = (mantissa.split('.')[1] || '').replace(/0+$/, '');
  return Math.max(0, fraction.length - Number(exponent));
}

export function marketNumber(value: unknown, increment?: unknown): string {
  if (value === undefined || value === null || value === '') return '—';
  const digits = incrementDigits(increment);
  if (digits !== undefined && digits <= 20) return number(value, digits);
  // Until instrument metadata is verified, preserve the exchange precision instead of guessing.
  const text = String(value);
  if (!/^-?\d+(\.\d+)?$/.test(text)) return number(value, 20);
  const [integer, fraction] = text.split('.');
  return (
    integer.replace(/\B(?=(\d{3})+(?!\d))/g, ',') +
    (fraction === undefined ? '' : `.${fraction}`)
  );
}

export function instrumentUnits(definition?: Row) {
  const family = String(definition?.uly || definition?.instFamily || '').split(
    '-',
  );
  const base = definition?.baseCcy || family[0] || '单位待确认';
  // OKX option px is the premium in settlement coin, not the USD strike unit.
  const quote =
    (definition?.instType === 'OPTION'
      ? definition.settleCcy
      : definition?.quoteCcy || family[1]) || '单位待确认';
  const contract = ['SWAP', 'FUTURES', 'OPTION'].includes(definition?.instType);
  return {
    base,
    quote,
    size: contract ? '张' : base,
    contractValue:
      contract && definition?.ctVal && definition?.ctValCcy
        ? `每张 ${definition.ctVal}${definition.ctMult && Number(definition.ctMult) !== 1 ? ` × ${definition.ctMult}` : ''} ${definition.ctValCcy}`
        : undefined,
    volumeCurrency: contract ? base : quote,
    volumeLabel: contract ? '24h 成交量' : '24h 成交额',
  };
}

export function instrumentType(instrument: string) {
  if (instrument.endsWith('-SWAP')) return 'SWAP';
  if (/-[CP]$/.test(instrument)) return 'OPTION';
  return /-\d{6}$/.test(instrument) ? 'FUTURES' : 'SPOT';
}

import { describe, expect, it } from 'vitest';
import { canAutoQuery, operationDefaults } from './defaults';

describe('product-specific API forms', () => {
  const schema = {
    type: 'object',
    properties: { tdMode: { type: 'string' }, instType: { type: 'string' } },
  };
  it('does not force cash mode onto derivatives', () => {
    expect(operationDefaults(schema, '/trade/spot/orders')).toEqual({
      tdMode: 'cash',
      instType: 'SPOT',
    });
    expect(operationDefaults(schema, '/trade/swap/orders')).toEqual({
      tdMode: 'cross',
      instType: 'SWAP',
    });
    expect(operationDefaults(schema, '/trade/option/orders')).toEqual({
      tdMode: 'cross',
      instType: 'OPTION',
    });
    expect(operationDefaults(schema, '/settings/developer/api')).toEqual({});
  });
  it('preserves published defaults and waits for required inputs', () => {
    expect(
      operationDefaults(
        { ...schema, properties: { tdMode: { default: 'isolated' } } },
        '/trade/swap/orders',
      ).tdMode,
    ).toBe('isolated');
    expect(canAutoQuery({ ...schema, required: ['instId'] }, {})).toBe(false);
    expect(canAutoQuery(schema, {})).toBe(true);
    expect(canAutoQuery(undefined, {})).toBe(false);
  });
});

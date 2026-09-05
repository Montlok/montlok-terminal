import { describe, expect, it } from 'vitest';
import mcp from '../../server/catalog/okx_mcp_tools.json';
import rest from '../../server/catalog/okx_rest_endpoints.json';
import { dataOf } from './api';
import { operationLabel } from './labels';
import { destination, navigation, pages } from './navigation';
import { BODY, formArguments } from './SchemaFields';

const tools = mcp.tools.map((tool) => ({ ...tool, kind: 'mcp' }));
const routes = rest.map((route) => ({
  ...route,
  kind: 'rest',
  name: `${route.method} ${route.path}`,
}));
describe('business navigation', () => {
  it('has six primary sections and unique three-level pages', () => {
    expect(navigation.map((item) => item.name)).toEqual([
      '行情',
      '交易',
      '资产',
      '策略',
      '引擎',
      '设置',
    ]);
    expect(new Set(pages.map((page) => page.path)).size).toBe(pages.length);
    expect(pages.every((page) => page.path.split('/').length === 4)).toBe(true);
  });
  it('maps every catalog entry to an actual business form', () => {
    const destinations = new Map(pages.map((page) => [page.path, page]));
    const missing = [...tools, ...routes].filter(
      (item) =>
        !destinations.has(destination(item)) ||
        destinations.get(destination(item))?.component,
    );
    expect(missing.map((item) => item.name)).toEqual([]);
    expect(
      [...tools, ...routes].filter(
        (item) => destination(item) === '/settings/developer/api',
      ),
    ).toEqual([]);
  });
  it('has a concise Chinese name for each MCP action', () => {
    expect(
      tools
        .filter((tool) => !/[\u4e00-\u9fff]/.test(operationLabel(tool)))
        .map((tool) => tool.name),
    ).toEqual([]);
  });
  it('keeps risk-bearing account actions in their business sections', () => {
    expect(destination({ name: 'account_transfer' })).toBe(
      '/assets/funds/transfer',
    );
    expect(destination({ name: 'spot_cancel_order' })).toBe(
      '/trade/spot/orders',
    );
    expect(destination({ name: 'grid_stop_order' })).toBe(
      '/strategies/bots/grid',
    );
  });
});
describe('API field forms', () => {
  it('unwraps official MCP metadata before displaying account rows', () => {
    expect(
      dataOf({
        result: {
          content: [
            {
              type: 'text',
              text: JSON.stringify({
                data: {
                  endpoint: 'GET /api/v5/trade/orders-pending',
                  requestTime: '2026-09-05',
                  data: [],
                },
              }),
            },
          ],
        },
      }),
    ).toEqual([]);
  });
  it('provides 378 documented or parameterless REST forms', () => {
    expect(routes.filter((route) => 'inputSchema' in route)).toHaveLength(378);
    expect(
      routes
        .filter(
          (route) => route.detail_level !== 'dedicated HTTP Request section',
        )
        .every((route) => !('inputSchema' in route)),
    ).toBe(true);
  });
  it('preserves decimal strings and false; does not send environment overrides', () => {
    const schema = {
      type: 'object',
      properties: {
        sz: { type: 'string' },
        reduceOnly: { type: 'boolean' },
        demo: { type: 'boolean' },
        px: { type: 'string' },
      },
    };
    expect(
      formArguments(
        { sz: '0.00001000', reduceOnly: false, demo: false, px: '' },
        schema,
      ),
    ).toEqual({ sz: '0.00001000', reduceOnly: false });
  });
  it('serializes top-level REST batch arrays and nested fields', () => {
    const schema = {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          instId: { type: 'string' },
          details: {
            type: 'array',
            items: { type: 'object', properties: { px: { type: 'string' } } },
          },
        },
      },
    };
    expect(
      formArguments(
        { [BODY]: [{ instId: 'BTC-USDT', details: [{ px: '100.00' }] }] },
        schema,
      ),
    ).toEqual([{ instId: 'BTC-USDT', details: [{ px: '100.00' }] }]);
  });
  it('serializes unknown object schema from the advanced object editor', () => {
    expect(
      formArguments(
        { params: '{"period":20}' },
        { type: 'object', properties: { params: { type: 'object' } } },
      ),
    ).toEqual({ params: { period: 20 } });
  });
});

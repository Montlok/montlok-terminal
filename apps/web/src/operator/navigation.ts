export type NavigationPage = { path: string; name: string; component?: string };
export type NavigationGroup = {
  path: string;
  name: string;
  pages: NavigationPage[];
};
export type NavigationSection = {
  path: string;
  name: string;
  icon: string;
  groups: NavigationGroup[];
};
const group = (
  base: string,
  name: string,
  pages: [string, string, string?][],
): NavigationGroup => ({
  path: base,
  name,
  pages: pages.map(([slug, label, component]) => ({
    path: `${base}/${slug}`,
    name: label,
    component,
  })),
});
export const navigation: NavigationSection[] = [
  {
    path: '/market',
    name: '行情',
    icon: 'LineChartOutlined',
    groups: [
      group('/market/prices', '市场', [
        ['quotes', '行情'],
        ['depth', '订单簿'],
        ['candles', 'K 线'],
        ['instruments', '交易品种'],
      ]),
      group('/market/data', '数据', [
        ['derivatives', '合约数据'],
        ['statistics', '市场统计'],
        ['indicators', '技术指标'],
        ['screen', '筛选器'],
      ]),
      group('/market/research', '资讯', [
        ['news', '新闻'],
        ['calendar', '财经日历'],
        ['sentiment', '市场情绪'],
        ['traders', '交易员'],
      ]),
    ],
  },
  {
    path: '/trade',
    name: '交易',
    icon: 'StockOutlined',
    groups: [
      group('/trade/spot', '现货', [
        ['terminal', '交易', './Terminal'],
        ['orders', '委托'],
        ['algo', '条件单'],
        ['fills', '成交'],
      ]),
      ...(['swap', 'futures', 'option'] as const).map((product) =>
        group(
          `/trade/${product}`,
          { swap: '永续', futures: '交割', option: '期权' }[product],
          [
            ['orders', '委托'],
            ['positions', '持仓'],
            ['algo', '条件单'],
            ['fills', '成交'],
          ],
        ),
      ),
      group('/trade/event', '事件合约', [
        ['orders', '委托'],
        ['positions', '市场'],
        ['fills', '成交'],
      ]),
      group('/trade/execution', '执行', [
        ['orders', '批量委托'],
        ['spread', '价差交易'],
        ['block', '大宗交易'],
      ]),
    ],
  },
  {
    path: '/assets',
    name: '资产',
    icon: 'WalletOutlined',
    groups: [
      group('/assets/account', '账户', [
        ['overview', '总览', './Account'],
        ['trading', '交易账户'],
        ['funding', '资金账户'],
        ['positions', '持仓'],
      ]),
      group('/assets/funds', '资金', [
        ['transfer', '划转'],
        ['deposit', '充币'],
        ['withdraw', '提币'],
        ['bills', '账单'],
        ['fees', '费率'],
        ['conversion', '兑换'],
      ]),
      group('/assets/earn', '赚币', [
        ['flexible', '活期'],
        ['fixed', '定期'],
        ['onchain', '链上赚币'],
        ['dual', '双币赢'],
        ['loan', '借贷'],
      ]),
    ],
  },
  {
    path: '/strategies',
    name: '策略',
    icon: 'FundProjectionScreenOutlined',
    groups: [
      group('/strategies/bots', '交易策略', [
        ['grid', '网格'],
        ['dca', '马丁格尔'],
        ['recurring', '定投'],
        ['signal', '信号策略'],
      ]),
      group('/strategies/copy', '跟单', [['trading', '跟单交易']]),
    ],
  },
  {
    path: '/engine',
    name: '引擎',
    icon: 'ClusterOutlined',
    groups: [
      group('/engine/run', '运行', [
        ['overview', '总览', './Engine'],
        ['positions', '持仓', './Engine'],
        ['orders', '委托', './Engine'],
        ['fills', '成交', './Engine'],
        ['strategies', '策略', './Engine'],
        ['config', '参数', './Engine'],
        ['health', '监控', './Engine'],
      ]),
    ],
  },
  {
    path: '/settings',
    name: '设置',
    icon: 'SettingOutlined',
    groups: [
      group('/settings/accounts', '账户', [
        ['connections', 'API 连接', './Connections'],
        ['preferences', '账户设置'],
        ['subaccounts', '子账户'],
      ]),
      group('/settings/system', '系统', [
        ['operations', '操作记录', './Operations'],
        ['requests', '接口记录'],
        ['status', '服务状态'],
        ['server', '宝塔面板', './Server'],
        ['skills', '扩展'],
        ['affiliate', '返佣'],
      ]),
      group('/settings/developer', '开发者', [
        ['api', '接口浏览器'],
        ['native', '引擎接口'],
      ]),
    ],
  },
];
export const pages = navigation.flatMap((section) =>
  section.groups.flatMap((group) => group.pages),
);
export const pageFor = (path: string) =>
  pages.find((page) => page.path === path);
export function breadcrumbs(path: string) {
  for (const section of navigation)
    for (const group of section.groups) {
      const page = group.pages.find((page) => page.path === path);
      if (page) return [section.name, group.name, page.name];
    }
  return [];
}

// Every API entry has one business destination; protocol selection stays in developer settings.
export function destination(item: {
  name: string;
  kind?: string;
  path?: string;
  section?: string;
}) {
  const name = item.name;
  const path = item.path || name.split(' ')[1] || '';
  if (name.startsWith('market_')) {
    if (/indicator/.test(name)) return '/market/data/indicators';
    if (/filter|spread/.test(name)) return '/market/data/screen';
    if (/instrument|stock_token/.test(name))
      return '/market/prices/instruments';
    if (/orderbook/.test(name)) return '/market/prices/depth';
    if (/candle/.test(name)) return '/market/prices/candles';
    if (/funding|mark_price|price_limit|interest|oi_history/.test(name))
      return '/market/data/derivatives';
    return '/market/prices/quotes';
  }
  if (name.startsWith('news_'))
    return `/market/research/${/calendar/.test(name) ? 'calendar' : /sentiment/.test(name) ? 'sentiment' : 'news'}`;
  if (name.startsWith('smartmoney_')) return '/market/research/traders';
  const product = name.match(/^(spot|swap|futures|option|event)_/);
  if (name === 'spot_set_leverage') return '/settings/accounts/preferences';
  if (product)
    return `/trade/${product[1]}/${/position|leverage|greeks|instruments|browse|series|events|markets/.test(name) ? 'positions' : /algo|move_stop/.test(name) ? 'algo' : /fills/.test(name) ? 'fills' : 'orders'}`;
  if (/^grid_|tradingBot\/grid/.test(name + path))
    return '/strategies/bots/grid';
  if (/^dca_|tradingBot\/dca/.test(name + path)) return '/strategies/bots/dca';
  if (/tradingBot\/recurring/.test(path)) return '/strategies/bots/recurring';
  if (/tradingBot\/signal/.test(path)) return '/strategies/bots/signal';
  if (/^onchain_|finance\/(staking|eth-staking|sol-staking)/.test(name + path))
    return '/assets/earn/onchain';
  if (/^dcd_|finance\/sfp\/dcd/.test(name + path)) return '/assets/earn/dual';
  if (/^earn_/.test(name))
    return `/assets/earn/${/fixed|flash/.test(name) ? 'fixed' : 'flexible'}`;
  if (/flexible-loan|borrow|loan|interest/.test(path))
    return '/assets/earn/loan';
  if (/\/finance\//.test(path))
    return `/assets/earn/${/savings/.test(path) ? 'flexible' : /dcd/.test(path) ? 'dual' : /staking|onchain/.test(path) ? 'onchain' : 'fixed'}`;
  if (/^skills_/.test(name)) return '/settings/system/skills';
  if (/^system_|\/system\/|\/support\//.test(name + path))
    return '/settings/system/status';
  if (/^trade_get_history/.test(name)) return '/settings/system/requests';
  if (/\/fiat\/buy-sell\//.test(path)) return '/assets/funds/conversion';
  if (/\/users\/glp\//.test(path)) return '/market/data/statistics';
  if (/subaccount|sub-account|subAcct/.test(path))
    return '/settings/accounts/subaccounts';
  if (/affiliate/.test(path)) return '/settings/system/affiliate';
  if (/copytrading/.test(path)) return '/strategies/copy/trading';
  if (/\/rfq\//.test(path)) return '/trade/execution/block';
  if (/\/sprd\//.test(path)) return '/trade/execution/spread';
  if (/\/trade\//.test(path)) return '/trade/execution/orders';
  if (/\/rubik\//.test(path)) return '/market/data/statistics';
  if (/\/market\//.test(path))
    return `/market/prices/${/books/.test(path) ? 'depth' : /candles/.test(path) ? 'candles' : 'quotes'}`;
  if (/\/public\//.test(path))
    return /instruments/.test(path)
      ? '/market/prices/instruments'
      : '/market/data/derivatives';
  if (/transfer/.test(name + path)) return '/assets/funds/transfer';
  if (/deposit/.test(path)) return '/assets/funds/deposit';
  if (/withdraw/.test(name + path)) return '/assets/funds/withdraw';
  if (/bills/.test(name + path)) return '/assets/funds/bills';
  if (/trade_fee|trade-fee/.test(name + path)) return '/assets/funds/fees';
  if (/positions|position-risk/.test(name + path))
    return '/assets/account/positions';
  if (/asset_balance|\/asset\//.test(name + path))
    return '/assets/account/funding';
  if (/balance|max_size|max_avail_size/.test(name + path))
    return '/assets/account/trading';
  if (/^account_|\/account\//.test(name + path))
    return '/settings/accounts/preferences';
  return '/settings/developer/api';
}

export type HealthLevel = 'HEALTHY' | 'DEGRADED' | 'CRITICAL';
export type TradingState = 'ACTIVE' | 'REDUCING' | 'HALTED' | 'RECOVERING';
export type OperatorCommand = 'PAUSE' | 'REDUCE_ONLY';

export interface SystemHealth {
  id: string;
  label: string;
  level: HealthLevel;
  detail: string;
}

export interface MarketSnapshot {
  instrumentId: string;
  venue: string;
  last: number;
  changePct: number;
  mark: number;
  high24h: number;
  low24h: number;
  volume24h: string;
  fundingRate: number;
  fundingCountdown: string;
  bid: number;
  ask: number;
}

export interface RiskSnapshot {
  netExposure: number;
  delta: number;
  margin: number;
  marginRatioPct: number;
  dailyPnl: number;
  dailyLossLimit: number;
  dailyLossUsedPct: number;
  queuePressurePct: number;
  lastReconcileAt: string;
  reconcileAge: string;
}

export interface PositionRow {
  strategy: string;
  instrument: string;
  side: 'Long' | 'Short' | 'Flat';
  quantity: number;
  averagePrice: number;
  markPrice: number;
  unrealizedPnl: number;
  notional: number;
  margin: number;
  state: string;
}

export interface OperatorException {
  id: string;
  severity: 'CRITICAL' | 'WARNING' | 'INFO';
  title: string;
  detail: string;
  firstSeen: string;
  age: string;
  status: 'OPEN' | 'ACK';
}

export interface OrderRow {
  id: string;
  strategy: string;
  instrument: string;
  side: 'Buy' | 'Sell';
  type: 'Limit' | 'Market' | 'Stop';
  quantity: number;
  price: number | null;
  status: 'OPEN' | 'PENDING' | 'HELD';
}

export interface StrategyInstance {
  id: string;
  artifact: string;
  configVersion: string;
  account: string;
  instrument: string;
  mode: 'ACTIVE' | 'REDUCING' | 'PAUSED';
  pnl: number;
  volume: number;
  heartbeat: string;
}

export interface RuntimeConfigEntry {
  key: string;
  value: string;
  previous: string;
  changed: boolean;
}

export interface OperatorSnapshot {
  schemaVersion: 1;
  observedAtNs: number;
  environment: 'DEMO' | 'LIVE';
  accountId: string;
  runId: string;
  tradingState: TradingState;
  readyToTrade: boolean;
  systems: SystemHealth[];
  market: MarketSnapshot;
  risk: RiskSnapshot;
  positions: PositionRow[];
  orders: OrderRow[];
  exceptions: OperatorException[];
  strategies: StrategyInstance[];
  runtimeConfig: RuntimeConfigEntry[];
  strategyArtifact: string;
  configVersion: string;
  eventStore: string;
  grafanaUrl: string;
  memoryPct: number;
  cpuPct: number;
}

export interface CommandReceipt {
  receiptId: string;
  status: 'ACCEPTED' | 'REJECTED';
  command: OperatorCommand;
  runId: string;
}

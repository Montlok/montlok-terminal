import { useModel } from '@umijs/max';
import { Alert, Button, Form, Input, Segmented } from 'antd';
import { useEffect, useState } from 'react';
import { api, number, type Row } from './api';
import { ConfirmOperation } from './ConfirmOperation';

export function OrderTicket({
  instrument,
  last,
  marketMode,
}: {
  instrument: string;
  last?: string;
  marketMode: string;
}) {
  const { account, refresh, epoch } = useModel('operator');
  const [side, setSide] = useState<'buy' | 'sell'>('buy');
  const [type, setType] = useState('limit');
  const [quantity, setQuantity] = useState('');
  const [price, setPrice] = useState('');
  const [ticket, setTicket] = useState<Row>();
  const [result, setResult] = useState<Row>();
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  useEffect(() => {
    setQuantity('');
    setPrice('');
    setTicket(undefined);
    setResult(undefined);
    setError('');
  }, [instrument, epoch]);
  const base = instrument.split('-')[0];
  const balance = account.balances.find(
    (row: Row) => row.ccy === (side === 'buy' ? 'USDT' : base),
  );
  async function prepare() {
    setBusy(true);
    setError('');
    setResult(undefined);
    try {
      if (!(Number(quantity) > 0)) throw new Error('输入交易数量');
      if (type === 'limit' && !(Number(price) > 0))
        throw new Error('输入委托价格');
      setTicket(
        await api('prepare', {
          kind: 'mcp',
          name: 'spot_place_order',
          arguments: {
            instId: instrument,
            tdMode: 'cash',
            tgtCcy: 'base_ccy',
            stpMode: 'cancel_taker',
            side,
            ordType: type,
            sz: quantity,
            ...(type === 'limit' ? { px: price } : {}),
          },
        }),
      );
    } catch (reason) {
      setError(String(reason));
    } finally {
      setBusy(false);
    }
  }
  return (
    <aside className="order-ticket panel">
      <div className="panel-heading">
        <strong>现货交易</strong>
        <span className="environment">
          {account.mode === 'demo' ? '模拟盘' : '只读'}
        </span>
      </div>
      <div className="ticket-body">
        <Segmented
          block
          aria-label="买卖方向"
          value={side}
          onChange={setSide}
          options={[
            { value: 'buy', label: '买入' },
            { value: 'sell', label: '卖出' },
          ]}
          className="side-buttons"
        />
        <Segmented
          block
          aria-label="委托类型"
          value={type}
          onChange={setType}
          options={[
            { value: 'limit', label: '限价' },
            { value: 'market', label: '市价' },
          ]}
          className="type-buttons"
        />
        <Form layout="vertical">
          {type === 'limit' && (
            <Form.Item label="价格 (USDT)" htmlFor="order-price">
              <Input
                id="order-price"
                inputMode="decimal"
                value={price}
                onChange={(event) => setPrice(event.target.value)}
                placeholder={last || '价格'}
                suffix={
                  <Button type="text" onClick={() => setPrice(last || '')}>
                    最新价
                  </Button>
                }
              />
            </Form.Item>
          )}
          <Form.Item label={`数量 (${base})`} htmlFor="order-quantity">
            <Input
              id="order-quantity"
              inputMode="decimal"
              value={quantity}
              onChange={(event) => setQuantity(event.target.value)}
              placeholder="数量"
            />
          </Form.Item>
        </Form>
        <div className="ticket-summary">
          <span>金额</span>
          <strong>
            {number(Number(quantity) * Number(type === 'limit' ? price : last))}{' '}
            USDT
          </strong>
        </div>
        <div className="ticket-summary">
          <span>可用</span>
          <strong>
            {number(balance?.availBal, 8)} {side === 'buy' ? 'USDT' : base}
          </strong>
        </div>
        <div className="ticket-summary">
          <span>交易模式</span>
          <span>现货</span>
        </div>
        <div className="ticket-summary">
          <span>行情</span>
          <span>{marketMode === 'live' ? '公开市场' : '模拟盘'}</span>
        </div>
        <Button
          block
          type="primary"
          className={`trade-submit ${side}`}
          loading={busy}
          disabled={!account.available || account.mode !== 'demo'}
          onClick={() => void prepare()}
        >
          {side === 'buy' ? '买入' : '卖出'} {base}
        </Button>
        {error && (
          <Alert type="error" title={error} className="inline-callout" />
        )}
        {result && (
          <Alert
            type={result.status === 'completed' ? 'success' : 'warning'}
            className="inline-callout"
            title={result.status === 'completed' ? '已提交' : '查看订单结果'}
            description={
              result.status === 'completed'
                ? undefined
                : JSON.stringify(result.result)
            }
          />
        )}
        <div className="ticket-divider" />
        <div className="ticket-summary">
          <span>账户连接</span>
          <span className={account.privateConnected ? 'positive' : 'muted'}>
            {account.privateConnected ? '正常' : '连接中'}
          </span>
        </div>
      </div>
      <ConfirmOperation
        ticket={ticket}
        onClose={() => setTicket(undefined)}
        onComplete={(value) => {
          setResult(value);
          void refresh();
        }}
      />
    </aside>
  );
}

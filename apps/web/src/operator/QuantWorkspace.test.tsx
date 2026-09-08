import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { useEffect, useState } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { QuantWorkspace } from './QuantWorkspace';
import { WORKSPACE_TABS_KEY } from './quantWorkspaceModel';

const routing = vi.hoisted(() => ({
  path: '/trade/spot/terminal',
  push: vi.fn(),
  mount: vi.fn(),
  unmount: vi.fn(),
}));
vi.mock('@umijs/max', () => ({
  history: { push: routing.push },
  useLocation: () => ({ pathname: routing.path }),
}));
vi.mock('./MarketDock', () => ({
  MarketDock: function TestMarketDock() {
    const [instrument, setInstrument] = useState('BTC-USDT');
    useEffect(() => {
      routing.mount();
      return routing.unmount;
    }, []);
    return (
      <input
        aria-label="persistent instrument"
        value={instrument}
        onChange={(event) => setInstrument(event.target.value)}
      />
    );
  },
}));
vi.mock('./StrategyRunPanel', () => ({
  StrategyRunPanel: () => <p>independent strategy inspector</p>,
}));

beforeEach(() => {
  routing.path = '/trade/spot/terminal';
  localStorage.clear();
  vi.clearAllMocks();
  vi.spyOn(HTMLElement.prototype,'getBoundingClientRect').mockReturnValue(new DOMRect(0,0,1600,900));
  vi.stubGlobal('ResizeObserver',class {
    constructor(private callback:ResizeObserverCallback){}
    observe(target:Element){queueMicrotask(()=>this.callback([{target,contentRect:new DOMRect(0,0,1600,900),borderBoxSize:[{inlineSize:1600,blockSize:900}],contentBoxSize:[{inlineSize:1600,blockSize:900}],devicePixelContentBoxSize:[]}],this as unknown as ResizeObserver));}
    unobserve(){}
    disconnect(){}
  });
});
afterEach(()=>{cleanup();vi.restoreAllMocks();vi.unstubAllGlobals();});

describe('persistent quant workspace', () => {
  it('keeps the same market instance and selection when lower routes change', async () => {
    const view = render(
      <QuantWorkspace>
        <p>account blotter</p>
      </QuantWorkspace>,
    );
    const input = await screen.findByLabelText('persistent instrument');
    fireEvent.change(input, { target: { value: 'ETH-USDT' } });
    routing.path = '/strategies/bots/grid';
    view.rerender(
      <QuantWorkspace>
        <p>grid detail</p>
      </QuantWorkspace>,
    );
    expect(screen.getByLabelText('persistent instrument')).toBe(input);
    expect(input).toHaveValue('ETH-USDT');
    expect(screen.getByText('grid detail')).toBeInTheDocument();
    expect(screen.queryByText('account blotter')).not.toBeInTheDocument();
    expect(routing.mount).toHaveBeenCalledTimes(1);
    expect(routing.unmount).not.toHaveBeenCalled();
    routing.path = '/assets/earn/loan';
    view.rerender(
      <QuantWorkspace>
        <p>loan detail</p>
      </QuantWorkspace>,
    );
    expect(screen.getByLabelText('persistent instrument')).toBe(input);
    expect(routing.mount).toHaveBeenCalledTimes(1);
    view.unmount();
    expect(routing.unmount).toHaveBeenCalledTimes(1);
  });
  it('opens routed tabs, links selections to navigation and closes the active tab', async () => {
    routing.path = '/strategies/bots/grid';
    const view = render(
      <QuantWorkspace>
        <p>grid detail</p>
      </QuantWorkspace>,
    );
    await screen.findByLabelText('persistent instrument');
    expect(
      screen.getByRole('tab', { name: '组合 · 总览' }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole('tab', { name: /交易策略 · 网格/ }),
    ).toHaveAttribute('aria-selected', 'true');
    fireEvent.click(screen.getByRole('tab', { name: '策略组 · 总览' }));
    expect(routing.push).toHaveBeenLastCalledWith(
      '/strategies/groups/overview',
    );
    fireEvent.click(screen.getByRole('button', { name: 'remove' }));
    expect(routing.push).toHaveBeenLastCalledWith('/trade/spot/terminal');
    routing.path = '/trade/spot/terminal';
    view.rerender(
      <QuantWorkspace>
        <p>account blotter</p>
      </QuantWorkspace>,
    );
    expect(
      screen.queryByRole('tab', { name: /交易策略 · 网格/ }),
    ).not.toBeInTheDocument();
    expect(
      JSON.parse(localStorage.getItem(WORKSPACE_TABS_KEY) || '[]'),
    ).not.toContain('/strategies/bots/grid');
  });
  it('renders the active page and an independently docked inspector', async () => {
    const { container } = render(
      <QuantWorkspace>
        <p>one active page</p>
      </QuantWorkspace>,
    );
    await screen.findByLabelText('persistent instrument');
    expect(container.querySelector('.flexlayout__layout')).toBeInTheDocument();
    expect(container.querySelectorAll('.flexlayout__tabset')).toHaveLength(3);
    expect(
      container.querySelector('.workspace-route-content'),
    ).toContainElement(screen.getByText('one active page'));
    expect(container.querySelector('.workspace-editor')).toBeInTheDocument();
    expect(screen.getByRole('complementary',{name:'策略控制'})).toContainElement(
      screen.getByText('independent strategy inspector'),
    );
  });
});

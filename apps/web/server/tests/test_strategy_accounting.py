import unittest
from strategy_accounting import cash_ledger, cash_snapshot


class CashAccountingTests(unittest.TestCase):
    def setUp(self):
        self.manifest = dict(execution='nautilus_sandbox', account_type='CASH',
            rebalance_policy='frozen_signals_initial_allocation', group_id='baseline',
            run_id='baseline-test', capital_usdt=200)
        self.fill = dict(id='fill-1', orderId='order-1', strategy='SECTOR-000',
            instrument='XNVDA-USDT.OKX', side='BUY', quantity='0.5', price='200',
            fee='0.1', feeCurrency='USDT', time='2026-09-07T00:00:01Z')
        self.view = dict(fills=[self.fill], fillsTotal=1)

    def test_reconciliation_cash_does_not_double_count_owned_positions(self):
        snapshot = dict(fills=1, cash_usdt=199.9, nav_usdt=299.9,
            positions=[dict(instrument='XNVDA-USDT.OKX', quantity=.5, bid_mark=200)])
        result = cash_snapshot(cash_ledger(self.manifest, self.view), snapshot)
        self.assertEqual(result['nav_usdt'], 199.9)
        self.assertEqual(result['pnl_usdt'], -.1)
        self.assertEqual(result['cash_usdt'], 99.9)

    def test_curve_observation_uses_only_fills_present_at_that_time(self):
        ledger = cash_ledger(self.manifest, self.view)
        self.assertEqual(cash_snapshot(ledger, dict(fills=0, positions=[]))['nav_usdt'], 200)

    def test_incomplete_or_duplicate_ledger_has_no_performance(self):
        for view in (dict(fills=[], fillsTotal=1), dict(fills=[self.fill, self.fill], fillsTotal=2)):
            self.assertFalse(cash_ledger(self.manifest, view)['valid'])

    def test_inventory_mismatch_has_no_performance(self):
        ledger = cash_ledger(self.manifest, self.view)
        self.assertIsNone(cash_snapshot(ledger, dict(fills=1, positions=[])))

    def test_buy_sell_cycle_accounts_for_costs(self):
        sell = {**self.fill, 'id': 'fill-2', 'side': 'SELL', 'price': '220',
                'fee': '.11', 'time': '2026-09-07T01:00:00Z'}
        ledger = cash_ledger(self.manifest, dict(fills=[sell, self.fill], fillsTotal=2))
        result = cash_snapshot(ledger, dict(fills=2, positions=[]))
        self.assertEqual(result['nav_usdt'], 209.79)
        self.assertEqual(result['pnl_usdt'], 9.79)

    def test_other_execution_modes_keep_their_own_accounting(self):
        for manifest in ({**self.manifest, 'execution': 'okx_live'},
                         {**self.manifest, 'rebalance_policy': 'model_signals'}):
            self.assertIsNone(cash_ledger(manifest, self.view))

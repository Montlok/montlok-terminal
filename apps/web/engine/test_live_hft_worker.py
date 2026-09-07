import json
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import time
import unittest
from unittest.mock import patch

from live_hft_worker import Runtime, owned_orders, risk_notional_limits, run_deadline, registered, sha


class LiveAccountingTests(unittest.TestCase):
    def test_continuous_risk_cap_is_absolute_and_not_frozen_to_startup_price(self):
        snapshot = {
            'executionSettings': {'mode': 'continuous_quotes', 'maxOrderNotionalUsdt': 10},
            'pairs': [{'instrument': 'XPLTR-USDT', 'tradeSize': '.01', 'ask': '176.01'}],
        }
        self.assertEqual(risk_notional_limits(snapshot), {'XPLTR-USDT.OKX': Decimal('10')})
        snapshot['pairs'][0]['ask'] = '350'
        self.assertEqual(risk_notional_limits(snapshot), {'XPLTR-USDT.OKX': Decimal('10')})

    def test_single_allocation_risk_cap_remains_bounded_to_its_startup_order(self):
        snapshot = {'pairs': [{'instrument': 'XPLTR-USDT', 'tradeSize': '.01', 'ask': '176.01'}]}
        self.assertEqual(
            risk_notional_limits(snapshot),
            {'XPLTR-USDT.OKX': Decimal('.01') * Decimal('176.01') * Decimal('1.02')},
        )

    def test_capital_parameters_are_not_forced_to_one_usdt_or_half_the_account(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            worker = root / 'worker.py'; worker.write_text('# local configuration fixture')
            config = json.loads((Path(__file__).parents[1] / 'deploy/live-hft.json').read_text())
            config.update(capitalUtilization=0.05, maxQuoteExposureUsdt='25', preserveInitialInventory=True)
            config_path = root / 'config.json'
            registry_path = root / 'registry.json'
            spec = dict(id='live-capital', kind='live', environment='live', workerPath=str(worker),
                        workerSha256=sha(worker), configPath=str(config_path))
            args = SimpleNamespace(registry=registry_path, group_id=spec['id'])
            for cap in ('1', '25', '250000'):
                config['maxQuoteExposureUsdt'] = cap
                config_path.write_text(json.dumps(config)); spec['configSha256'] = sha(config_path)
                registry_path.write_text(json.dumps({'groups': [spec]}))
                self.assertEqual(registered(args, worker)[2]['maxQuoteExposureUsdt'], cap)
            for cap in ('0', '-1', 'NaN', 'Infinity'):
                config['maxQuoteExposureUsdt'] = cap
                config_path.write_text(json.dumps(config)); spec['configSha256'] = sha(config_path)
                registry_path.write_text(json.dumps({'groups': [spec]}))
                with self.assertRaises(ValueError):
                    registered(args, worker)

    def test_startup_reconciliation_lookback_is_explicitly_bounded(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            worker = root / 'worker.py'; worker.write_text('# local configuration fixture')
            config = json.loads((Path(__file__).parents[1] / 'deploy/live-hft.json').read_text())
            config_path = root / 'config.json'
            registry_path = root / 'registry.json'
            spec = dict(id='live-reconcile', kind='live', environment='live', workerPath=str(worker),
                        workerSha256=sha(worker), configPath=str(config_path))
            args = SimpleNamespace(registry=registry_path, group_id=spec['id'])
            for minutes in (0, 1, 60):
                config['startupReconciliationLookbackMins'] = minutes
                config_path.write_text(json.dumps(config)); spec['configSha256'] = sha(config_path)
                registry_path.write_text(json.dumps({'groups': [spec]}))
                self.assertEqual(registered(args, worker)[2]['startupReconciliationLookbackMins'], minutes)
            for minutes in (-1, 61, True, '1'):
                config['startupReconciliationLookbackMins'] = minutes
                config_path.write_text(json.dumps(config)); spec['configSha256'] = sha(config_path)
                registry_path.write_text(json.dumps({'groups': [spec]}))
                with self.assertRaises(ValueError):
                    registered(args, worker)

    def test_continuous_run_has_no_timer_deadline(self):
        spec = {'durationPolicyVersion': 1, 'maxDurationSeconds': None}
        self.assertIsNone(run_deadline({'durationSeconds': 0}, spec, 1234))
        self.assertEqual(run_deadline({'durationSeconds': 604800}, spec, 1234), 606034)
        self.assertEqual(run_deadline({'durationSeconds': 60}, spec, 1234), 1294)
        for duration in (-1, True, 0.5, None, '0'):
            with self.assertRaises(ValueError):
                run_deadline({'durationSeconds': duration}, spec, 1234)
        with self.assertRaises(ValueError):
            run_deadline({'durationSeconds': 0}, {'maxDurationSeconds': 60}, 1234)
        with self.assertRaises(ValueError):
            run_deadline({'durationSeconds': 61}, {'maxDurationSeconds': 60}, 1234)

    def test_reconciled_history_never_counts_as_this_run(self):
        prior = SimpleNamespace(strategy_id='EXTERNAL', filled_qty='1')
        current = SimpleNamespace(strategy_id='EXEC-CHECK', filled_qty='0')
        node = SimpleNamespace(cache=SimpleNamespace(orders=lambda: [prior, current]))
        self.assertEqual(owned_orders(node), [current])

    def test_fill_fees_and_shutdown_state_are_reported_from_execution(self):
        pair = dict(instrument='XRKLB-USDT', base='XRKLB', baseAvailable='0', bid='64.5', ask='64.5')
        fee = SimpleNamespace(as_decimal=lambda: 0.000014, currency='XRKLB')
        fill = SimpleNamespace(trade_id='trade-1', client_order_id='ABC123', venue_order_id='987',
            instrument_id='XRKLB-USDT.OKX', order_side='BUY', last_qty='0.014', last_px='64.5',
            commission=fee, ts_event=1)
        order = SimpleNamespace(client_order_id='ABC123', venue_order_id='987', strategy_id='TEST',
            instrument_id='XRKLB-USDT.OKX', side='BUY', quantity='0.014', filled_qty='0.014',
            leaves_qty='0', price='64.5', avg_px=64.5, status='FILLED', ts_init=1,
            commissions=lambda: {'XRKLB': fee}, events=lambda: [fill])
        quote = SimpleNamespace(ts_event=time.time_ns(), bid_price='64.5', ask_price='64.5')
        cache = SimpleNamespace(quote=lambda _: quote, account_for_venue=lambda _: object(),
            orders=lambda: [order], orders_total_count=lambda: 1, orders_open_count=lambda: 0)
        native = SimpleNamespace(InstrumentId=SimpleNamespace(from_str=lambda s: s), Venue=lambda s: s)
        with TemporaryDirectory() as directory, patch.dict('sys.modules', {
            'nautilus_trader': SimpleNamespace(), 'nautilus_trader.core': SimpleNamespace(nautilus_pyo3=native)}):
            runtime = Runtime(Path(directory), dict(runId='test-run', groupId='test'),
                              dict(profileId='real', pairs=[pair]), dict(maxQuoteExposureUsdt='1'))
            runtime.state = 'ACTIVE'
            runtime.update(SimpleNamespace(cache=cache, is_running=True), time.monotonic())
            result = runtime.status()
            self.assertAlmostEqual(result['nav_usdt'], 0.999097)
            self.assertAlmostEqual(result['fees_usdt'], 0.000903)
            self.assertEqual(result['ordersAccepted'], 1)
            self.assertEqual(result['ordersDenied'], 0)
            self.assertTrue(result['ordersEnabled'])
            positions = json.loads((Path(directory) / 'view.json').read_text())['positions']
            self.assertAlmostEqual(float(positions[0]['quantity']), 0.013986)
            runtime.state = 'STOPPED'
            runtime.update(SimpleNamespace(cache=cache, is_running=False), time.monotonic())
            self.assertFalse(runtime.status()['ordersEnabled'])
            self.assertEqual(runtime.status()['connected'], {'data': False, 'exec': False})
            self.assertEqual(len((Path(directory) / 'equity.jsonl').read_text().splitlines()), 2)


if __name__ == '__main__':
    unittest.main()

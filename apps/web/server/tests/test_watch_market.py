import asyncio
import unittest
from unittest.mock import AsyncMock

from watch_market import PublicWatchMarket,parse_symbols
from group_runtime import GroupRuntimeClient


class WatchMarketTests(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_tables_share_one_public_fetch_and_only_return_requested_symbols(self):
        fetch=AsyncMock(return_value=[{'instId':'XNVDA-USDT','last':'200'},{'instId':'XAAPL-USDT','last':'300'},{'instId':'BTC-USDT','last':'80000'}])
        cache=PublicWatchMarket(fetch)
        values=await asyncio.gather(*[cache.read(['XNVDA-USDT','XAAPL-USDT']) for _ in range(12)])
        fetch.assert_awaited_once()
        self.assertEqual(len(values[0]['tickers']),2)
        values[0]['tickers'][0]['last']='changed'
        self.assertEqual((await cache.read(['XNVDA-USDT']))['tickers'][0]['last'],'200')
        self.assertEqual((await cache.read(['XTSLA-USDT']))['missing'],['XTSLA-USDT'])
        cache.cached_at=0
        await cache.read(['XNVDA-USDT'])
        self.assertEqual(fetch.await_count,2)

    def test_symbol_bounds_and_duplicates(self):
        self.assertEqual(parse_symbols('XNVDA-USDT,XNVDA-USDT'),['XNVDA-USDT'])
        for value in ('','../../state','BTC-USDT-SWAP',','.join(f'X{i}-USDT' for i in range(65))):
            with self.assertRaises(ValueError):parse_symbols(value)

    async def test_failed_fetch_does_not_poison_following_queries(self):
        fetch=AsyncMock(side_effect=[ValueError('unavailable'),[{'instId':'BTC-USDT'}]])
        cache=PublicWatchMarket(fetch)
        with self.assertRaises(ValueError):await cache.read(['BTC-USDT'])
        self.assertEqual(len((await cache.read(['BTC-USDT']))['tickers']),1)

    async def test_runtime_status_is_coalesced_copied_and_invalidated_after_actions(self):
        client=GroupRuntimeClient(None)
        client.request=AsyncMock(return_value={'groups':[{'runDir':'server-private-path','runId':'one'}]})
        a,b=await asyncio.gather(client.status('alpha'),client.status('alpha'))
        client.request.assert_awaited_once()
        a['groups'][0].pop('runDir')
        self.assertIn('runDir',b['groups'][0])
        await client.status('alpha')
        self.assertEqual(client.request.await_count,1)
        await client.execute({'action':'stop'},'test-only')
        await client.status('alpha')
        self.assertEqual(client.request.await_count,3)

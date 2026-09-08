"""Browser acceptance over a labelled, read-only historical screenshot fixture.

The values below come from the operator's historical screenshot. Empty
positions, fills, and candle arrays remain empty because that evidence did not
contain synchronized records for them. This harness is never deployed.
"""
from __future__ import annotations
import json
import os
from pathlib import Path
from datetime import datetime
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import threading
from urllib.parse import urlsplit,parse_qs
from playwright.sync_api import sync_playwright

WEB = Path(__file__).resolve().parents[1]
FIXTURE = json.loads(Path(os.environ['MONTLOK_BROWSER_FIXTURE']).read_text())
GROUP = FIXTURE['group_id']
RUN = FIXTURE['run_id']
OBSERVED = int(datetime.fromisoformat(FIXTURE['observed_at']).timestamp())
PROFILE = FIXTURE['profile']
GROUP_DATA = {**FIXTURE['group'], 'observedAt': OBSERVED}
RUNTIME = FIXTURE['runtime']


def response(path,query=None):
    if path == '/api/session': return {'csrf': 'historical-browser-review-' * 3, 'operator': 'recorded-review', 'role': 'viewer', 'mode': 'live'}
    if path == '/api/profiles': return {'profiles': [PROFILE], 'epoch': 1}
    if path == '/api/account': return {'mode': 'live', 'available': False, 'balances': [], 'orders': [], 'fills': []}
    if path == '/api/catalog': return {'tools': [], 'routes': [], 'nativeMethods': []}
    if path == '/api/strategy-groups': return {'groups': [GROUP_DATA]}
    if path.endswith('/runtime'): return RUNTIME
    if path.endswith('/equity'): return {'id': GROUP, 'groupId': GROUP, 'runId': RUN, 'equity': [], 'drawdown': [], 'points': []}
    if path.startswith('/api/strategy-groups/'): return GROUP_DATA
    if path == '/api/market/snapshot':
        instrument=(query or {}).get('instrument',[''])[0]
        return {'ticker': FIXTURE.get('quotes',{}).get(instrument,{}),
                'book': FIXTURE.get('books',{}).get(instrument,{'bids':[],'asks':[]}),
                'trades':[], 'receivedAt':OBSERVED, 'source':'historical_screenshot'}
    if path == '/api/market/watchlist':
        return {'tickers':[{'instId':key,**value} for key,value in FIXTURE.get('quotes',{}).items()],
                'observedAt':OBSERVED,'missing':[]}
    if path in {'/api/query', '/api/market/candles'}: return {'data': []}
    if path == '/api/events': return None
    return {}


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs): super().__init__(*args, directory=str(WEB / 'dist'), **kwargs)
    def do_GET(self):
        if not (WEB / 'dist' / urlsplit(self.path).path.lstrip('/')).is_file(): self.path = '/index.html'
        super().do_GET()
    def log_message(self, *_): pass


def main():
    output = Path(os.environ.get('MONTLOK_BROWSER_OUTPUT', '/tmp/montlok-browser-review'))
    output.mkdir(parents=True, exist_ok=True)
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    writes = []; errors = []
    try:
        with sync_playwright() as playwright:
            chrome = os.environ.get('MONTLOK_BROWSER_EXECUTABLE', playwright.chromium.executable_path)
            browser = playwright.chromium.launch(executable_path=chrome)
            page = browser.new_page(viewport={'width': 1920, 'height': 1080}, device_scale_factor=1)
            page.on('pageerror', lambda error: errors.append(str(error)))
            page.route_web_socket('**/api/market/stream?**', lambda _: None)

            def api(route):
                path = urlsplit(route.request.url).path
                if route.request.method == 'POST' and path != '/api/query':
                    writes.append(path); route.fulfill(status=403, json={'error': 'Historical browser fixture'}); return
                value = response(path,parse_qs(urlsplit(route.request.url).query))
                if path=='/api/query' and route.request.post_data_json.get('name')=='GET /api/v5/public/instruments':
                    instrument=route.request.post_data_json.get('arguments',{}).get('instId','XTSLA-USDT')
                    value={'data':[{'instId':instrument,'instType':'SPOT','baseCcy':instrument.split('-')[0],'quoteCcy':'USDT'}]}
                if value is None:
                    route.fulfill(status=200, body=': historical fixture\n\n', content_type='text/event-stream')
                else: route.fulfill(status=200, json=value)

            page.route('**/api/**', api)
            page.goto(f'http://127.0.0.1:{server.server_port}/workspace/portfolio/overview')
            page.get_by_role('heading', name='实盘组合').wait_for(timeout=20000)
            page.get_by_label('可停靠终端工作区').wait_for()
            page.screenshot(path=str(output / 'workspace-1920.png'), full_page=True)
            page.get_by_role('tab', name='策略组 · 总览', exact=True).click()
            page.wait_for_url('**/strategies/groups/overview')
            assert page.locator('.market-dock').count() == 1
            assert not writes, writes
            for width, height in [(1440, 900), (2560, 1440)]:
                page.set_viewport_size({'width': width, 'height': height})
                page.screenshot(path=str(output / f'workspace-{width}.png'), full_page=True)
            assert not errors, errors
            print(json.dumps({'screenshots': str(output), 'page_errors': errors, 'mutation_requests': writes, 'viewport_checks': 3}))
            browser.close()
    finally:
        server.shutdown()


if __name__ == '__main__': main()

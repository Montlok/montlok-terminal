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
import gzip
import base64
import subprocess
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
            page.add_init_script("""window.__terminalPerf={lcp:0,cls:0,shifts:[],longTasks:[],interactions:[]};
                new PerformanceObserver(list=>{for(const e of list.getEntries())window.__terminalPerf.lcp=e.startTime;}).observe({type:'largest-contentful-paint',buffered:true});
                new PerformanceObserver(list=>{for(const e of list.getEntries())if(!e.hadRecentInput){window.__terminalPerf.cls+=e.value;window.__terminalPerf.shifts.push({value:e.value,time:e.startTime,sources:e.sources.map(s=>({node:s.node?.className,previous:s.previousRect,current:s.currentRect}))});}}).observe({type:'layout-shift',buffered:true});
                new PerformanceObserver(list=>{for(const e of list.getEntries())window.__terminalPerf.longTasks.push(e.duration);}).observe({type:'longtask',buffered:true});
                new PerformanceObserver(list=>{for(const e of list.getEntries())if(e.interactionId)window.__terminalPerf.interactions.push(e.duration);}).observe({type:'event',buffered:true,durationThreshold:16});
            """)
            page.on('pageerror', lambda error: errors.append(str(error)))
            page.route_web_socket('**/api/market/stream?**', lambda _: None)
            compiler="import {build} from 'esbuild';const result=await build({entryPoints:[process.argv[1]],bundle:true,format:'esm',platform:'node',write:false});await import('data:text/javascript;base64,'+Buffer.from(result.outputFiles[0].contents).toString('base64'));"
            contract=json.loads(subprocess.check_output(['node','--input-type=module','-e',compiler,str(WEB/'tests/eventFrames.mjs')],cwd=WEB.parents[1],env={**os.environ,'MONTLOK_EVENT_CONTEXT':json.dumps({'runId':RUN,'strategyGroupId':GROUP,'accountId':GROUP_DATA['accountId'],'instrumentId':'XTSLA-USDT'})}))
            def event_stream(socket):
                received=0
                def client_message(_):
                    nonlocal received
                    received+=1
                    if received==2:
                        for frame in contract['frames']: socket.send(base64.b64decode(frame))
                socket.on_message(client_message)
            page.route_web_socket('**/api/v2/stream',event_stream)

            def api(route):
                path = urlsplit(route.request.url).path
                if route.request.method == 'POST' and path != '/api/query':
                    writes.append(path); route.fulfill(status=403, json={'error': 'Historical browser fixture'}); return
                value = response(path,parse_qs(urlsplit(route.request.url).query))
                if path.startswith('/api/v2/events/contract-event-') and path.endswith('/related'): value=contract['related']
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
            page.wait_for_timeout(300)
            baseline=page.evaluate("({...window.__terminalPerf, scripts:performance.getEntriesByType('resource').filter(e=>new URL(e.name).pathname.endsWith('.js')).map(e=>({url:new URL(e.name).pathname,duration:e.duration,size:e.decodedBodySize})),navigation:performance.getEntriesByType('navigation').map(e=>({ttfb:e.responseStart,domContentLoaded:e.domContentLoadedEventEnd}))})")
            baseline['critical_js_gzip_bytes']=sum(len(gzip.compress((WEB/'dist'/item['url'].lstrip('/')).read_bytes())) for item in baseline['scripts'] if (WEB/'dist'/item['url'].lstrip('/')).is_file())
            (output/'performance.json').write_text(json.dumps(baseline,indent=2))
            page.screenshot(path=str(output / 'workspace-1920.png'), full_page=True)
            page.get_by_role('tab', name='策略组 · 总览', exact=True).click()
            page.wait_for_url('**/strategies/groups/overview')
            assert page.locator('.market-dock').count() == 1
            assert not writes, writes
            if page.get_by_role('button',name='查找功能',exact=False).count():
                page.keyboard.press('Meta+k')
                page.get_by_label('功能搜索',exact=True).fill('模型发布')
                page.get_by_label('功能搜索',exact=True).press('Enter')
                page.wait_for_url('**/strategies/resources/models')
                page.get_by_role('button',name='组合与风险',exact=True).click()
                page.wait_for_url('**/workspace/portfolio/overview')
                page.get_by_role('button',name='聚焦面板',exact=True).click()
                assert page.locator('.market-dock').count()==1
                page.get_by_role('button',name='聚焦面板',exact=True).click()
            for width, height in [(1440, 900), (2560, 1440)]:
                page.set_viewport_size({'width': width, 'height': height})
                page.screenshot(path=str(output / f'workspace-{width}.png'), full_page=True)
            if page.get_by_role('button',name='事件与订单',exact=True).count():
                page.get_by_role('button',name='事件与订单',exact=True).click()
                page.locator('.event-blotter-grid').get_by_text('成交',exact=True).click(timeout=15000)
                page.get_by_label('事件详情').get_by_text('contract-trade',exact=True).wait_for()
                page.screenshot(path=str(output/'event-detail.png'),full_page=True)
                page.get_by_role('tab',name='关联时间线',exact=True).click()
                page.locator('.event-timeline').get_by_text('委托提交',exact=True).wait_for()
                assert page.locator('.event-timeline>div').count()==3
                page.screenshot(path=str(output/'event-timeline.png'),full_page=True)
            page.goto(f'http://127.0.0.1:{server.server_port}/workspace/execution/overview')
            page.get_by_role('button', name='分组与透视', exact=True).click(timeout=20000)
            try:
                page.locator('perspective-viewer').wait_for(state='visible',timeout=30000)
            except Exception:
                page.screenshot(path=str(output/'perspective-failure.png'),full_page=True)
                print(json.dumps({'page_errors':errors,'alerts':page.get_by_role('alert').all_text_contents()},ensure_ascii=False))
                raise
            page.screenshot(path=str(output / 'perspective-detail.png'),full_page=True)
            assert not errors, errors
            print(json.dumps({'screenshots': str(output), 'page_errors': errors, 'mutation_requests': writes, 'viewport_checks': 3}))
            browser.close()
    finally:
        server.shutdown()


if __name__ == '__main__': main()

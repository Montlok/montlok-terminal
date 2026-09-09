"""Local capacity/restart checks over the actual Rust executables and NATS.

Only recorded-format event files are produced. No exchange client is started.
The temporary state is deliberately outside the repository and production host.
"""
from __future__ import annotations
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import tempfile
import time
import urllib.request
import socketserver
import threading

ROOT = Path(__file__).resolve().parents[3]


def port():
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        return probe.getsockname()[1]


def wait_until(predicate, timeout=30):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            result = predicate()
            if result:
                return result
        except (OSError, sqlite3.Error):
            pass
        time.sleep(.025)
    raise AssertionError('Pipeline did not reach the expected watermark')


def count(path, table):
    if not path.is_file():
        return 0
    with sqlite3.connect(path.as_uri() + '?mode=ro', uri=True) as db:
        return db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]


def run():
    nats = Path(os.environ['MONTLOK_NATS_SERVER_BIN']).resolve()
    binaries = Path(os.environ.get('MONTLOK_BIN_DIR', str(ROOT / 'target/debug')))
    state = Path(tempfile.mkdtemp(prefix='montlok-pipeline-')).resolve()
    nats_port, gateway_port = port(), port()
    key = 'local-recording-reader-' + os.urandom(24).hex()
    token_path = state / 'read-token'; token_path.write_text(key); token_path.chmod(0o600)
    log = state / 'execution-events.jsonl'; log.touch()
    processes = []
    watcher_mode=os.environ.get('MONTLOK_TEST_WATCHER')=='1'
    supervisor=None
    status_runs=[]
    if watcher_mode:
        run_root=state/'runs';run_directory=run_root/'recorded-run';run_directory.mkdir(parents=True)
        log=run_directory/'execution-events.jsonl';log.touch()
        (run_directory/'manifest.json').write_text(json.dumps({'mode':'live','run_id':'recorded-run','group_id':'recorded-alpha','account_id':'recorded-account'}))
        status_runs.append({'runId':'recorded-run','status':'running','runDir':str(run_directory)})
        class StatusOnly(socketserver.StreamRequestHandler):
            def handle(self):
                request=json.loads(self.rfile.readline())
                assert request=={'command':'status'},request
                self.wfile.write(json.dumps({'ok':True,'result':{'groups':[{'mode':'live','groupId':'recorded-alpha','profileId':'recorded-account','runs':status_runs}]}}).encode()+b'\n')
        supervisor=socketserver.UnixStreamServer(str(state/'supervisor.sock'),StatusOnly)
        threading.Thread(target=supervisor.serve_forever,daemon=True).start()

    def launch(name, command):
        output = (state / f'{name}.log').open('ab')
        process = subprocess.Popen(command, cwd=state, stdout=output, stderr=subprocess.STDOUT)
        output.close(); processes.append(process); return process

    def gateway():
        return launch('gateway', [str(binaries / 'montlok-gateway'), '--bind', f'127.0.0.1:{gateway_port}',
            '--database', str(state / 'gateway.sqlite'), '--nats-url', f'nats://127.0.0.1:{nats_port}',
            '--read-token-file', str(token_path)])

    def write_events(start, stop):
        with log.open('a') as stream:
            for n in range(start, stop):
                record = {'type': 'submitted', 'orderId': f'recorded-order-{n}', 'instrument': 'XTSLA-USDT.OKX',
                          'side': 'BUY' if n % 2 else 'SELL', 'quantity': '0.001', 'price': '354.48',
                          'time': 1800000000000000000 + n, 'source': 'recording.capacity-check'}
                stream.write(json.dumps(record, separators=(',', ':')) + '\n')
            stream.flush(); os.fsync(stream.fileno())

    try:
        launch('nats', [str(nats), '--addr', '127.0.0.1', '--port', str(nats_port), '--jetstream', '--store_dir', str(state / 'jetstream')])
        wait_until(lambda: socket.create_connection(('127.0.0.1', nats_port), timeout=.1).close() is None)
        running_gateway = gateway()
        launch('projector', [str(binaries / 'montlok-projector'), '--nats-url', f'nats://127.0.0.1:{nats_port}',
                            '--database', str(state / 'projector.sqlite'), '--parquet-root', str(state / 'parquet')])
        if watcher_mode:
            launch('watcher',[str(binaries/'montlok-run-watcher'),'--socket',str(state/'supervisor.sock'),'--run-root',str(run_root),'--state-root',str(state/'adapters'),'--adapter',str(binaries/'montlok-nautilus-adapter'),'--nats-url',f'nats://127.0.0.1:{nats_port}'])
        else: launch('adapter', [str(binaries / 'montlok-nautilus-adapter'), '--event-log', str(log), '--database', str(state / 'outbox.sqlite'),
                          '--nats-url', f'nats://127.0.0.1:{nats_port}', '--account-id', 'recorded-account', '--group-id', 'recorded-alpha', '--run-id', 'recorded-run'])
        boot = time.monotonic(); write_events(1, 101)
        wait_until(lambda: count(state / 'gateway.events.sqlite', 'event_log') == 100)
        wait_until(lambda: count(state / 'projector.sqlite', 'events') == 100)
        startup = time.monotonic() - boot
        started = time.monotonic(); write_events(101, 10101)
        wait_until(lambda: count(state / 'gateway.events.sqlite', 'event_log') == 10100, timeout=90)
        wait_until(lambda: count(state / 'projector.sqlite', 'events') == 10100, timeout=90)
        elapsed = time.monotonic() - started
        running_gateway.kill(); running_gateway.wait(timeout=5)
        write_events(10101, 10201)
        wait_until(lambda: count(state / 'projector.sqlite', 'events') == 10200)
        recovered = gateway()
        wait_until(lambda: count(state / 'gateway.events.sqlite', 'event_log') == 10200)
        request = urllib.request.Request(f'http://127.0.0.1:{gateway_port}/api/v2/events?stream=run.recorded-run&after_seq=10100',
                                         headers={'Authorization': 'Bearer ' + key})
        with urllib.request.urlopen(request, timeout=5) as response:
            events = json.load(response)
        assert [int(event['stream_seq']) for event in events] == list(range(10101, 10201))
        assert recovered.poll() is None
        if watcher_mode:
            next_directory=run_root/'recorded-next';next_directory.mkdir()
            (next_directory/'manifest.json').write_text(json.dumps({'mode':'live','run_id':'recorded-next','group_id':'recorded-alpha','account_id':'recorded-account'}))
            (next_directory/'execution-events.jsonl').write_text(json.dumps({'type':'submitted','orderId':'next-order','instrument':'XTSLA-USDT.OKX','side':'BUY','quantity':'0.001','price':'354.48','time':1800000000000020000})+'\n')
            status_runs[0]['status']='stopped'
            status_runs.append({'runId':'recorded-next','status':'running','runDir':str(next_directory)})
            wait_until(lambda:count(state/'gateway.events.sqlite','event_log')==10201)
            wait_until(lambda:'completed_run_indexed' in (state/'watcher.log').read_text())
        detail_times=[]
        for event in events[:30]:
            request=urllib.request.Request(f'http://127.0.0.1:{gateway_port}/api/v2/events/{event["event_id"]}/related',headers={'Authorization':'Bearer '+key})
            began=time.perf_counter()
            with urllib.request.urlopen(request,timeout=5) as response: detail=json.load(response)
            detail_times.append((time.perf_counter()-began)*1000)
            assert detail['encoding']=='protobuf-base64' and len(detail['events'])==1 and not detail['truncated']
        report = {'events': 10000, 'startup_seconds': round(startup,3), 'pipeline_seconds': round(elapsed, 3), 'events_per_second': round(10000 / elapsed),
                  'gateway_after_restart': 10200, 'projector_events': 10200, 'replay_count': len(events),'automatic_run_rollover':watcher_mode,
                  'detail_http_p50_ms':round(sorted(detail_times)[14],3),'detail_http_p95_ms':round(sorted(detail_times)[28],3), 'state_dir': str(state)}
        print(json.dumps(report, ensure_ascii=False))
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
                try: process.wait(timeout=5)
                except subprocess.TimeoutExpired: process.kill(); process.wait()
        if supervisor:supervisor.shutdown();supervisor.server_close()


if __name__ == '__main__':
    run()

"""Publish dashboard controls and a selectable frozen Alpha release; never start a run.

Run as root on the existing host with a staged operator_terminal tree. Web and
engine publication are separate: an active strategy prevents engine replacement.
Connection credentials, active profiles and trading requests are never modified.
"""
import argparse
import hashlib
import grp
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess

ROOT = Path('/www/nautilus/operator')
REGISTRY = Path('/etc/montlok-groups/registry.json')
ACTIVE = ('starting','running','halted','reducing','stopping','recovering','unresponsive','error','engine_stopped')


def checksum(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def command(*args):
    subprocess.run([str(arg) for arg in args], check=True, capture_output=True, text=True)


def active_runs():
    with sqlite3.connect('file:/www/nautilus/group-runs/runs.sqlite?mode=ro', uri=True) as db:
        return db.execute('SELECT id,state FROM runs WHERE state IN ('+','.join('?' for _ in ACTIVE)+')', ACTIVE).fetchall()


def install(source, target, mode=0o644):
    target=Path(target);target.parent.mkdir(parents=True,exist_ok=True)
    temporary=target.with_name(target.name+'.publishing')
    shutil.copy2(source,temporary);temporary.chmod(mode);temporary.replace(target)


def publish_web(source, backup):
    with sqlite3.connect(f'file:{ROOT}/state/operations.sqlite?mode=ro', uri=True) as db:
        if db.execute("SELECT count(*) FROM operations WHERE status='processing'").fetchone()[0]:
            raise RuntimeError('A user operation is in progress')
    server_files=('app.py','profiles.py','exchange.py','group_runtime.py','group_views.py','managed_run_view.py','strategy_accounting.py')
    backup.mkdir(parents=True,exist_ok=False)
    shutil.copytree(ROOT/'server',backup/'server')
    shutil.copy2(ROOT/'dashboard/index.html',backup/'index.html')
    # Fingerprinted assets are added before the entrypoint. Existing tabs retain
    # their previous chunks while the new document loads only the new release.
    for file in (source/'dist').rglob('*'):
        if file.is_file() and file.name!='index.html':
            install(file,ROOT/'dashboard'/file.relative_to(source/'dist'))
    for name in server_files:
        install(source/'server'/name,ROOT/'server'/name)
    install(source/'dist/index.html',ROOT/'dashboard/index.html')
    try:
        command('systemctl','restart','nautilus-operator-terminal')
        command('systemctl','is-active','nautilus-operator-terminal')
    except Exception:
        for name in server_files:
            if (backup/'server'/name).exists():install(backup/'server'/name,ROOT/'server'/name)
        install(backup/'index.html',ROOT/'dashboard/index.html')
        command('systemctl','restart','nautilus-operator-terminal')
        raise
    # The SSH-visible source copy is for ongoing user development, not secrets.
    checkout=Path('/home/gabira/montlok-dashboard')
    if checkout.is_dir():
        import pwd
        owner=pwd.getpwnam('gabira')
        for directory in ('src','server','engine','deploy'):
            for file in (source/directory).rglob('*'):
                if file.is_file() and '__pycache__' not in file.parts:
                    target=checkout/directory/file.relative_to(source/directory)
                    install(file,target);os.chown(target,owner.pw_uid,owner.pw_gid)
    return dict(web='published',backup=str(backup),connectionProfiles='unchanged',tradingRuns='unchanged')


def publish_engine(source, backup):
    running=active_runs()
    if running:raise RuntimeError('Active runs retained; engine publication deferred: '+json.dumps(running))
    registry=json.loads(REGISTRY.read_text())
    signal_path=Path('/etc/montlok-groups/baseline.signals.json')
    signals=json.loads(signal_path.read_text())
    if signals.get('strategy')!='sector_regime_core_60_momentum_40' or len(signals['instruments'])!=25:
        raise RuntimeError('Frozen Alpha release differs')
    release=Path('/usr/local/lib/montlok-live/releases/sector-6040-20260907-v2')
    release.mkdir(parents=True,exist_ok=True)
    for name in ('live_alpha_worker.py','live_hft_worker.py','control.py'):
        install(source/'engine'/name,release/name)
    config=json.loads(Path('/etc/montlok-live/hft.json').read_text())
    config['instruments']=sorted({key.removesuffix('.OKX') for key in signals['instruments']} | {'XSPCX-USDT'})
    config.update(capitalUtilization=1,marketDataAgeSecs=5,maxOrderSubmitRate='8/00:00:01')
    for name in ('maxQuoteExposureUsdt','preserveInitialInventory','useQuoteBalance','executionCheck','executionCheckSide','executionCheckQuantity','inventoryFloor'):
        config.pop(name,None)
    config_path=release/'config.json';config_path.write_text(json.dumps(config,indent=2)+'\n')
    config_path.chmod(0o640);os.chown(config_path,0,grp.getgrnam('nautilus').gr_gid)
    template=next(row for row in registry['groups'] if row['id']=='live-hft-inventory')
    spec={**template,'id':'live-sector-6040','name':'四板块 60/40 · 实盘',
          'description':'板块趋势与 20 日动量 · 60% 基础篮子 / 40% 动量优选',
          'workerPath':str(release/'live_alpha_worker.py'),'workerSha256':checksum(release/'live_alpha_worker.py'),
          'configPath':str(config_path),'configSha256':checksum(config_path),
          'signalsPath':str(signal_path),'signalsSha256':checksum(signal_path),
          'environment':'live','kind':'live','enabled':True,'capitalMode':'account_inventory','maxDurationSeconds':86400}
    spec.pop('exposureCapUsdt',None)
    registry['groups']=[row for row in registry['groups'] if row['id']!=spec['id']]+[spec]
    backup.mkdir(parents=True,exist_ok=False)
    shutil.copy2(REGISTRY,backup/'registry.json')
    targets={Path(registry['workerPath']):source/'engine/group_worker.py',
             Path('/usr/local/lib/montlok-live/engine/live_hft_worker.py'):source/'engine/live_hft_worker.py',
             Path('/usr/local/lib/montlok-model-preopen-20260906/operator_terminal/engine/live_hft_worker.py'):source/'engine/live_hft_worker.py',
             Path('/usr/local/lib/montlok-groups/server/group_runtime.py'):source/'server/group_runtime.py'}
    for index,(target,origin) in enumerate(targets.items()):
        if target.exists():shutil.copy2(target,backup/f'file-{index}.py')
        install(origin,target)
    for row in registry['groups']:
        if row.get('kind')=='live':row['workerSha256']=checksum(row['workerPath'])
    candidate=REGISTRY.with_name('registry.dashboard-controls.json')
    candidate.write_text(json.dumps(registry,indent=2)+'\n');candidate.chmod(0o640);os.chown(candidate,0,grp.getgrnam('nautilus').gr_gid)
    # Validate the complete registration without creating a trading node.
    command('/opt/montlok-runtime/bin/python','-c',
        "import sys;sys.path.insert(0,'/usr/local/lib/montlok-groups/server');from group_runtime import LaunchRegistry;LaunchRegistry(__import__('pathlib').Path(sys.argv[1]))",str(candidate))
    if active_runs():raise RuntimeError('A run began during staging; registration has not been activated')
    candidate.replace(REGISTRY)
    command('systemctl','restart','nautilus-group-supervisor')
    command('systemctl','is-active','nautilus-group-supervisor')
    return dict(engine='published',groupId=spec['id'],strategy=signals['strategy'],createdRuns=0,backup=str(backup))


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--source',type=Path,required=True)
    parser.add_argument('--part',choices=('web','engine'),required=True)
    parser.add_argument('--backup',type=Path,required=True);args=parser.parse_args()
    if os.geteuid()!=0:raise SystemExit('Run on the deployment host as root')
    result=(publish_web if args.part=='web' else publish_engine)(args.source.resolve(),args.backup)
    print(json.dumps(result,ensure_ascii=False))

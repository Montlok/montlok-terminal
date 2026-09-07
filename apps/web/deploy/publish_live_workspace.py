"""Publish the existing live workspace and native controls without starting runs.

Requires an idle supervisor and a successful offline native smoke test. The
previous source, native library, registration, entrypoint and encrypted profile
are retained together for rollback. All exchange checks here are read-only.
"""
import argparse
import asyncio
import grp
import hashlib
import json
import os
from pathlib import Path
import pwd
import shutil
import sqlite3
import subprocess
import sys
import time

ROOT=Path('/www/nautilus/operator')
REGISTRY=Path('/etc/montlok-groups/registry.json')
NATIVE=Path('/usr/local/lib/montlok-model-preopen-20260906/runtime-source/nautilus_trader/core/nautilus_pyo3.cpython-312-x86_64-linux-gnu.so')
BUILT=Path('/www/nautilus/git/nautilus-weilan/target/release/libnautilus_pyo3.so')
ACTIVE=('starting','running','halted','reducing','stopping','recovering','unresponsive','error','engine_stopped')


def command(*values):
    return subprocess.run([str(v) for v in values],check=True,capture_output=True,text=True).stdout


def digest(path):
    with Path(path).open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def idle():
    import psutil
    for process in psutil.process_iter(['pid','cmdline']):
        args=process.info['cmdline'] or []
        if '--request' in args and '--preview' not in args and any(Path(arg).name in {'live_hft_worker.py','live_model_worker.py','live_alpha_worker.py'} for arg in args):
            raise RuntimeError('已有实盘进程保留，本次发布未执行')
    with sqlite3.connect('file:/www/nautilus/group-runs/runs.sqlite?mode=ro',uri=True) as db:
        runs=db.execute('SELECT id,state FROM runs WHERE state IN ('+','.join('?' for _ in ACTIVE)+')',ACTIVE).fetchall()
    if runs:raise RuntimeError('已有运行保留，本次发布未执行: '+json.dumps(runs))
    with sqlite3.connect(f'file:{ROOT}/state/operations.sqlite?mode=ro',uri=True) as db:
        if db.execute("SELECT count(*) FROM operations WHERE status='processing'").fetchone()[0]:
            raise RuntimeError('页面操作仍在处理')


def main(source,backup):
    if os.geteuid()!=0:raise RuntimeError('Run on the existing deployment host as root')
    idle()
    if not (source/'dist/index.html').is_file() or not BUILT.is_file():raise RuntimeError('发布文件缺失')
    native_smoke=json.loads(Path('/tmp/montlok-full-control-test/result.json').read_text())
    if native_smoke.get('nativeControl')!='passed' or native_smoke.get('ordersSubmitted')!=0 or native_smoke.get('binaryMtimeNs')!=BUILT.stat().st_mtime_ns:
        raise RuntimeError('原生控制验证未完成')
    sys.path.insert(0,str(source/'server'))
    from profiles import Profiles
    from exchange import verify
    import aiohttp
    profiles=Profiles(ROOT/'state',None)
    previous=profiles.get()
    if previous['id']!='tokyoreal' or previous['site']!='global' or previous['mode'] not in ('live','live_readonly'):
        raise RuntimeError('当前连接与待发布工作区不一致')
    profile={**previous,'mode':'live'}
    async def check():
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
            return await verify(session,profile)
    verification=asyncio.run(check())
    if str(verification.get('uid'))!=str((previous.get('verification') or {}).get('uid')) or 'trade' not in verification.get('permissions','').split(','):
        raise RuntimeError('实盘账户交易权限核验不一致')
    backup.mkdir(parents=True,exist_ok=False);backup.chmod(0o700)
    saved={}
    def retain(path):
        path=Path(path)
        if str(path) in saved:return
        if path.exists():
            copy=backup/f'file-{len(saved):03d}'
            shutil.copy2(path,copy)
            saved[str(path)]={'copy':str(copy),'mode':path.stat().st_mode&0o777,'uid':path.stat().st_uid,'gid':path.stat().st_gid}
        else:saved[str(path)]=None
    def install(origin,target,mode=0o644,uid=0,gid=0):
        target=Path(target);retain(target);target.parent.mkdir(parents=True,exist_ok=True)
        temporary=target.with_name(target.name+'.publishing')
        shutil.copy2(origin,temporary);temporary.chmod(mode);os.chown(temporary,uid,gid);temporary.replace(target)
    registry=json.loads(REGISTRY.read_text())
    retain(REGISTRY);retain(ROOT/'state/profiles.enc');retain(ROOT/'dashboard/index.html');retain(NATIVE)
    uid=pwd.getpwnam('nautilus').pw_uid;gid=grp.getgrnam('nautilus').gr_gid
    # Prevent new UI starts while an idle runtime registration is replaced.
    idle();command('systemctl','stop','nautilus-operator-terminal');command('systemctl','stop','nautilus-group-supervisor')
    try:
        idle()
        install(BUILT,NATIVE,0o755)
        engine_dirs={Path(row['workerPath']).parent for row in registry['groups'] if row.get('kind')=='live'}
        live_model=registry.get('modelRuntime',{}).get('liveExecution')
        if live_model:engine_dirs.add(Path(live_model['workerPath']).parent)
        for directory in engine_dirs:
            if not str(directory).startswith('/usr/local/lib/montlok-'):
                raise RuntimeError('运行目录不在既有安装范围')
            for name in ('live_hft_worker.py','live_controls.py','control.py'):
                install(source/'engine'/name,directory/name)
            if (directory/'live_alpha_worker.py').exists():install(source/'engine/live_alpha_worker.py',directory/'live_alpha_worker.py')
            if (directory/'live_model_worker.py').exists():install(source/'engine/live_model_worker.py',directory/'live_model_worker.py')
        for row in registry['groups']:
            if row.get('kind')=='live':
                row['controlVersion']=1
                row['workerSha256']=row['strategySha256']=digest(row['workerPath'])
                if row['id']=='live-yesterday-alpha-test-1u':row['dashboardVisible']=False
        if live_model:
            live_model['workerSha256']=live_model['strategySha256']=digest(live_model['workerPath'])
            live_model['controlVersion']=1
            config=json.loads(Path(live_model['configPath']).read_text())
            for key in ('maxQuoteExposureUsdt','preserveInitialInventory','useQuoteBalance','executionCheck','executionCheckSide','executionCheckQuantity','inventoryFloor'):
                config.pop(key,None)
            config.update(capitalUtilization=1,instruments=['BTC-USDT'])
            config_path=Path('/etc/montlok-live/rdt-cpu.json');retain(config_path)
            temporary=backup/'rdt-config.json';temporary.write_text(json.dumps(config,indent=2)+'\n')
            install(temporary,config_path,0o640,0,gid)
            live_model.update(configPath=str(config_path),configSha256=digest(config_path),capitalMode='account_inventory')
            live_model.pop('exposureCapUsdt',None)
        supervisor=Path('/usr/local/lib/montlok-groups/server/group_runtime.py')
        install(source/'server/group_runtime.py',supervisor)
        archived=backup/'registry-candidate.json';archived.write_text(json.dumps(registry,indent=2)+'\n')
        # The registration's entire ancestry must be root-owned, including the
        # candidate; the web data tree is intentionally owned by nautilus.
        candidate=REGISTRY.with_name('registry-live-workspace.next.json')
        install(archived,candidate,0o640,0,gid)
        command('/opt/montlok-runtime/bin/python','-c',
            "import sys;sys.path.insert(0,'/usr/local/lib/montlok-groups/server');from group_runtime import LaunchRegistry;LaunchRegistry(__import__('pathlib').Path(sys.argv[1]))",candidate)
        install(candidate,REGISTRY,0o640,0,gid)
        command('runuser','-u','nautilus','--','/opt/montlok-runtime/bin/python','-c',
            "import sys;sys.path.insert(0,'/usr/local/lib/montlok-groups/server');from group_runtime import LaunchRegistry;LaunchRegistry(__import__('pathlib').Path('/etc/montlok-groups/registry.json'))")
        for file in (source/'server').glob('*.py'):install(file,ROOT/'server'/file.name)
        for file in (source/'dist').rglob('*'):
            if file.is_file() and file.name!='index.html':
                target=ROOT/'dashboard'/file.relative_to(source/'dist')
                target.parent.mkdir(parents=True,exist_ok=True)
                shutil.copy2(file,target);target.chmod(0o644)
        profiles.save(profile,verification)
        os.chown(ROOT/'state/profiles.enc',uid,gid)
        install(source/'dist/index.html',ROOT/'dashboard/index.html')
        command('systemctl','start','nautilus-group-supervisor','nautilus-operator-terminal')
        command('systemctl','is-active','nautilus-group-supervisor','nautilus-operator-terminal')
        owner=pwd.getpwnam('gabira');checkout=Path('/home/gabira/montlok-dashboard')
        if checkout.is_dir():
            for dirname in ('src','server','engine','deploy'):
                for file in (source/dirname).rglob('*'):
                    if file.is_file() and '__pycache__' not in file.parts:
                        target=checkout/dirname/file.relative_to(source/dirname)
                        target.parent.mkdir(parents=True,exist_ok=True)
                        os.chown(target.parent,owner.pw_uid,owner.pw_gid)
                        shutil.copy2(file,target);os.chown(target,owner.pw_uid,owner.pw_gid)
        result={'published':True,'profile':'tokyoreal','mode':'live','nativeControlVersion':1,
            'newTradingRuns':0,'exchangeMutations':0,'backup':str(backup),'at':time.time()}
        (backup/'result.json').write_text(json.dumps(result,indent=2)+'\n')
        print(json.dumps(result,ensure_ascii=False))
    except Exception:
        for target,record in saved.items():
            if record:
                temp=Path(target+'.rollback');shutil.copy2(record['copy'],temp)
                temp.chmod(record['mode']);os.chown(temp,record['uid'],record['gid']);temp.replace(target)
        command('systemctl','restart','nautilus-group-supervisor','nautilus-operator-terminal')
        raise
    finally:
        (backup/'rollback-files.json').write_text(json.dumps(saved,indent=2)+'\n')


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--source',type=Path,required=True)
    parser.add_argument('--backup',type=Path,required=True);args=parser.parse_args()
    main(args.source.resolve(),args.backup)

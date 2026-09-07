"""Publish only the web application; preserve the strategy supervisor and workers."""
import argparse
import gzip
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess

ROOT=Path('/www/nautilus/operator')
PUBLIC=Path('/www/wwwroot/tokyo.montlok.com/app')
FILES=('app.py','group_runtime.py','group_views.py','watch_market.py')

def command(*args):return subprocess.run(args,check=True,capture_output=True,text=True).stdout.strip()
def processing():
    with sqlite3.connect(f'file:{ROOT}/state/operations.sqlite?mode=ro',uri=True) as db:
        return db.execute("SELECT count(*) FROM operations WHERE status='processing'").fetchone()[0]
def install(source,target):
    target=Path(target);target.parent.mkdir(parents=True,exist_ok=True)
    temp=target.with_name(target.name+'.publishing')
    shutil.copy2(source,temp);temp.chmod(0o644);temp.replace(target)

def install_asset(source,relative):
    for root in (ROOT/'dashboard',PUBLIC):
        target=root/relative
        install(source,target)
        if source.suffix in {'.js','.css','.svg'} and source.stat().st_size>512:
            compressed=target.with_name(target.name+'.gz')
            temporary=compressed.with_name(compressed.name+'.publishing')
            with source.open('rb') as incoming,temporary.open('wb') as raw:
                with gzip.GzipFile(fileobj=raw,mode='wb',compresslevel=9,mtime=0) as outgoing:
                    shutil.copyfileobj(incoming,outgoing)
            temporary.chmod(0o644);temporary.replace(compressed)

def publish(source,backup):
    if os.geteuid()!=0:raise RuntimeError('Run on the deployment host as root')
    if processing():raise RuntimeError('页面操作正在处理，本次未发布')
    before=command('systemctl','show','nautilus-group-supervisor','-p','MainPID','--value')
    backup.mkdir(parents=True,exist_ok=False);backup.chmod(0o700)
    for name in FILES:
        if (ROOT/'server'/name).exists():shutil.copy2(ROOT/'server'/name,backup/name)
    shutil.copy2(ROOT/'dashboard/index.html',backup/'index.html')
    if (PUBLIC/'index.html').is_file():shutil.copy2(PUBLIC/'index.html',backup/'public-index.html')
    for file in (source/'dist').rglob('*'):
        if file.is_file() and file.name not in {'index.html','stats.json'}:install_asset(file,file.relative_to(source/'dist'))
    if processing():raise RuntimeError('有新操作正在处理，网页入口未切换')
    command('systemctl','stop','nautilus-operator-terminal')
    try:
        for name in FILES:install(source/'server'/name,ROOT/'server'/name)
        install_asset(source/'dist/index.html',Path('index.html'))
        command('systemctl','start','nautilus-operator-terminal')
        command('systemctl','is-active','nautilus-operator-terminal')
        after=command('systemctl','show','nautilus-group-supervisor','-p','MainPID','--value')
        if after!=before:raise RuntimeError('策略服务进程发生变化，需核对外部变更')
        result={'published':True,'scope':'web-only','supervisorPidBefore':before,'supervisorPidAfter':after,
            'entry':re.search(r'umi\.[a-f0-9]+\.js',(ROOT/'dashboard/index.html').read_text()).group(),
            'backup':str(backup),'tradingRequests':0,'engineFilesChanged':0}
        (backup/'result.json').write_text(json.dumps(result,indent=2)+'\n')
        print(json.dumps(result))
    except Exception:
        for name in FILES:
            if (backup/name).exists():install(backup/name,ROOT/'server'/name)
        install(backup/'index.html',ROOT/'dashboard/index.html')
        if (backup/'public-index.html').is_file():install(backup/'public-index.html',PUBLIC/'index.html')
        command('systemctl','restart','nautilus-operator-terminal')
        raise

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--source',type=Path,required=True);parser.add_argument('--backup',type=Path,required=True)
    args=parser.parse_args();publish(args.source.resolve(),args.backup)

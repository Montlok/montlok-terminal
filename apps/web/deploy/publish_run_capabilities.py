"""Publish live run controls without creating or changing a trading run."""
import argparse
import grp
import hashlib
import json
import os
from pathlib import Path
import pwd
import re
import shutil
import sqlite3
import subprocess

ROOT = Path('/www/nautilus/operator')
REGISTRY = Path('/etc/montlok-groups/registry.json')
SUPERVISOR = Path('/usr/local/lib/montlok-groups/server/group_runtime.py')
WORKERS = (
    Path('/usr/local/lib/montlok-live/engine/live_hft_worker.py'),
    Path('/usr/local/lib/montlok-live/releases/sector-6040-20260907-v2/live_hft_worker.py'),
    Path('/usr/local/lib/montlok-model-preopen-20260906/operator_terminal/engine/live_hft_worker.py'),
)


def command(*args):
    return subprocess.run([str(arg) for arg in args], check=True, capture_output=True, text=True).stdout.strip()


def require_idle():
    with sqlite3.connect('file:/www/nautilus/group-runs/runs.sqlite?mode=ro', uri=True) as db:
        rows = db.execute("SELECT id,state FROM runs WHERE state NOT IN ('completed','stopped','failed','interrupted')").fetchall()
    if rows:
        raise RuntimeError('已有运行保持不变，本次未替换运行程序: ' + json.dumps(rows))
    with sqlite3.connect(f'file:{ROOT}/state/operations.sqlite?mode=ro', uri=True) as db:
        if db.execute("SELECT COUNT(*) FROM operations WHERE status='processing'").fetchone()[0]:
            raise RuntimeError('有页面操作正在处理，本次未发布')


def install(source, target):
    target = Path(target)
    info = target.stat() if target.exists() else None
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + '.publishing')
    shutil.copy2(source, temporary)
    temporary.chmod(info.st_mode & 0o777 if info else 0o644)
    if info:
        os.chown(temporary, info.st_uid, info.st_gid)
    temporary.replace(target)


def publish(source, backup):
    if os.geteuid() != 0:
        raise RuntimeError('Use the deployment account')
    require_idle()
    registry = json.loads(REGISTRY.read_text())
    targets = {
        ROOT / 'server/app.py': source / 'server/app.py',
        ROOT / 'server/group_runtime.py': source / 'server/group_runtime.py',
        SUPERVISOR: source / 'server/group_runtime.py',
        **{target: source / 'engine/live_hft_worker.py' for target in WORKERS},
        ROOT / 'dashboard/index.html': source / 'dist/index.html',
    }
    for target, origin in targets.items():
        if not target.is_file() or not origin.is_file():
            raise RuntimeError(f'Missing deployed or release file: {target}')
    backup.mkdir(parents=True, exist_ok=False)
    backup.chmod(0o700)
    restore = {}
    for i, target in enumerate([*targets, REGISTRY]):
        saved = backup / f'{i}-{target.name}'
        shutil.copy2(target, saved)
        restore[target] = saved
    # Add immutable chunks before switching the document. Existing tabs keep
    # their chunks and session credentials remain untouched.
    for origin in (source / 'dist').rglob('*'):
        if origin.is_file() and origin.name != 'index.html':
            install(origin, ROOT / 'dashboard' / origin.relative_to(source / 'dist'))
    web_stopped = supervisor_stopped = modified = False
    try:
        command('systemctl', 'stop', 'nautilus-operator-terminal')
        web_stopped = True
        require_idle()
        command('systemctl', 'stop', 'nautilus-group-supervisor')
        supervisor_stopped = True
        modified = True
        for target, origin in targets.items():
            install(origin, target)
        for spec in registry['groups']:
            if spec.get('kind') != 'live':
                continue
            spec['workerSha256'] = hashlib.sha256(Path(spec['workerPath']).read_bytes()).hexdigest()
            if spec.get('dashboardVisible', True) and spec.get('exposureCapUsdt') is None:
                spec.update(durationPolicyVersion=1, maxDurationSeconds=None)
                if Path(spec['workerPath']).name == 'live_alpha_worker.py':
                    spec['executionPolicy'] = 'initial_allocation'
                elif Path(spec['workerPath']).name == 'live_hft_worker.py':
                    spec['executionPolicy'] = 'inventory_quotes'
        live_model = registry.get('modelRuntime', {}).get('liveExecution')
        if live_model:
            live_model.update(durationPolicyVersion=1, maxDurationSeconds=None, executionPolicy='model_signal')
        candidate = REGISTRY.with_name('registry.run-capabilities.json')
        candidate.write_text(json.dumps(registry, indent=2) + '\n')
        candidate.chmod(0o640)
        os.chown(candidate, 0, grp.getgrnam('nautilus').gr_gid)
        command('/opt/montlok-runtime/bin/python', '-c',
                "import sys;from pathlib import Path;sys.path.insert(0,'/usr/local/lib/montlok-groups/server');from group_runtime import LaunchRegistry;LaunchRegistry(Path(sys.argv[1]))",
                candidate)
        candidate.replace(REGISTRY)
        command('systemctl', 'start', 'nautilus-group-supervisor')
        command('systemctl', 'is-active', 'nautilus-group-supervisor')
        supervisor_stopped = False
        command('systemctl', 'start', 'nautilus-operator-terminal')
        command('systemctl', 'is-active', 'nautilus-operator-terminal')
        web_stopped = False
    except Exception:
        if modified:
            for target, saved in restore.items():
                install(saved, target)
            command('systemctl', 'restart', 'nautilus-group-supervisor')
            command('systemctl', 'restart', 'nautilus-operator-terminal')
        raise
    finally:
        if supervisor_stopped:
            command('systemctl', 'start', 'nautilus-group-supervisor')
        if web_stopped:
            command('systemctl', 'start', 'nautilus-operator-terminal')
    checkout = Path('/home/gabira/montlok-dashboard')
    owner = pwd.getpwnam('gabira')
    for part in ('src', 'server', 'engine', 'deploy'):
        for origin in (source / part).rglob('*'):
            if origin.is_file():
                target = checkout / part / origin.relative_to(source / part)
                install(origin, target)
                os.chown(target, owner.pw_uid, owner.pw_gid)
    print(json.dumps({'published': True, 'entry': re.search(r'umi\.[a-f0-9]+\.js', (ROOT / 'dashboard/index.html').read_text()).group(),
                      'backup': str(backup), 'createdRuns': 0, 'tradingRequests': 0,
                      'profiles': 'unchanged', 'strategyParameters': 'unchanged'}, ensure_ascii=False))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--backup', type=Path, required=True)
    args = parser.parse_args()
    publish(args.source.resolve(), args.backup)

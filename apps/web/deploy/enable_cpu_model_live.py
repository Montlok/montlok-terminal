#!/usr/bin/env python3
"""Bind published CPU models to the installed native live worker and account."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import time


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


registry_path = Path('/etc/montlok-groups/registry.json')
document = json.loads(registry_path.read_text())
engine = Path('/usr/local/lib/montlok-model-preopen-20260906/operator_terminal/engine')
config_path = Path('/etc/montlok-live/rdt-cpu-1u.json')
config = json.loads(Path('/etc/montlok-live/yesterday-alpha-test-1u.json').read_text())
config['instruments'] = ['BTC-USDT']
for key in ('executionCheck', 'executionCheckSide', 'executionCheckQuantity', 'inventoryFloor'):
    config.pop(key, None)
config_path.write_text(json.dumps(config, indent=2) + '\n')
config_path.chmod(0o444)
for group in document['groups']:
    if group.get('kind') == 'live':
        group['workerSha256'] = digest(group['workerPath'])
        group['strategySha256'] = group['workerSha256']
        group['configSha256'] = digest(group['configPath'])
        if group['id'] == 'live-yesterday-alpha-test-1u':
            group['name'] = '实盘执行验证 · 1 USDT'
            symbol = json.loads(Path(group['configPath']).read_text())['instruments'][0]
            group['description'] = symbol + ' 限价 IOC；单次提交；金额上限 1 USDT'
models = document['modelRuntime']
models.setdefault('contractKey', {})['rdt4quant_cpu_v2'] = 'crypto:BTC-USDT'
models['runnerRoots'] = {'rdt4quant_cpu_v2': '/usr/local/lib/montlok-cpp/releases/20260906-v2'}
models['liveExecution'] = {
    'workerPath': str(engine / 'live_model_worker.py'),
    'workerSha256': digest(engine / 'live_model_worker.py'),
    'strategySha256': digest(engine / 'live_model_worker.py'),
    'configPath': str(config_path), 'configSha256': digest(config_path),
    'profileStatePath': '/www/nautilus/operator/state', 'profileId': 'tokyoreal',
    'operatorServerPath': '/usr/local/lib/montlok-live/operator-api',
    'capitalMode': 'incremental_quote_cap', 'exposureCapUsdt': '1', 'dashboardVisible': True,
}
temporary = registry_path.with_suffix('.cpu-next.json')
temporary.write_text(json.dumps(document, ensure_ascii=False, indent=2) + '\n')
temporary.chmod(0o644)
sys.path.insert(0, '/usr/local/lib/montlok-groups/server')
from group_runtime import LaunchRegistry
checked = LaunchRegistry(temporary)
live_models = [g for g in checked.groups.values() if g.get('modelHash') and g.get('kind') == 'live']
if not live_models:
    raise ValueError('No published CPU model is bound to the live worker')
shutil.copy2(registry_path, registry_path.with_name('registry.before-cpu-live-' + str(int(time.time())) + '.json'))
os.replace(temporary, registry_path)
print(json.dumps({'liveModels': [{'id': g['id'], 'name': g['name'], 'mode': g['mode']} for g in live_models]}))

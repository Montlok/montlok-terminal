#!/usr/bin/env python3
"""Publish the installed montlok.cpp CPU checkpoint through the model catalog."""
from pathlib import Path
import hashlib
import json
import os
import shutil
import sys
import zipfile

import torch

sys.path.insert(0, '/www/nautilus/operator/server')
from artifacts import ArtifactStore
from model_releases import ModelReleaseStore, validate_manifest


def checksum(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


root = Path('/usr/local/lib/montlok-cpp/releases/20260906-v2')
candidate = root / 'assets'
checkpoint = candidate / 'best.pt'
c = torch.load(checkpoint, map_location='cpu', weights_only=True)
if c.get('epoch_complete') is not True:
    raise ValueError('Checkpoint does not contain a completed epoch')
data = json.loads((candidate / 'manifest.json').read_text())
files = [p for p in (root / 'rdt4quant/native').rglob('*.py')
         if '__pycache__' not in p.parts and 'tests' not in p.parts]
files += [root / 'rdt4quant' / n for n in ('prepare.py', 'download.py')]
files += [p for p in (root / 'rdt4quant_cpu').iterdir()
          if p.suffix in {'.py', '.h', '.cpp', '.so'} and p.name != 'build_montlok.py']
records = [{'path': 'source/' + str(p.relative_to(root)), 'sha256': checksum(p)} for p in sorted(files)]
manifest = {
    'schemaVersion': 1, 'releaseId': 'rdt4quant-cpu-20260906-v2', 'runnerId': 'rdt4quant_cpu_v2',
    'family': 'rdt4quant_multiasset', 'modelVersion': 'RDT4quant v1 / montlok.cpp v2',
    'model': {'path': 'model.pt', 'sha256': checksum(checkpoint)}, 'sources': records,
    'domainContracts': {'crypto:BTC-USDT': {
        'names': data['features'], 'sequenceBars': c['base_config']['sequence_length'], 'barSeconds': 60,
        'requiredMarkets': ['BTC-USDT', 'BTC-USDT-SWAP', 'ETH-USDT'],
        'mean': data['mean'], 'scale': data['std'], 'clip': [-12, 12], 'assetId': 0,
        'yScale': data['y_scale'], 'domain': 'crypto', 'instrument': 'BTC-USDT'}},
    'outputContract': {'kind': 'log_return_bps_quantiles', 'quantiles': [0.1, 0.5, 0.9],
        'horizons': {'crypto': c['config']['horizons']['crypto']},
        'horizonUnit': {'crypto': c['config']['horizon_units']['crypto']},
        'selectedHeadByDomain': {'crypto': 0}, 'depth': 4},
    'runtime': {'device': 'cpu', 'threads': 8, 'maxBatchSize': 8, 'maxQueueSize': 16,
                'timeoutMs': 2000, 'maxInputAgeMs': 120000},
    'policy': {'allowedModes': ['live'], 'thresholdBps': 12, 'maxTargetFraction': 1},
    'provenance': {'independentTestMetrics': None, 'fullEpoch': c['epoch'],
        'assetOrder': c['asset_order'], 'backend': 'montlok.cpp v2',
        'coverage': c.get('coverage'), 'trainingSignature': c.get('signature')},
}
validate_manifest(manifest)
bundle = Path('/tmp/rdt4quant-cpu-20260906-v2.zip')
with zipfile.ZipFile(bundle, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=1) as archive:
    archive.writestr('manifest.json', json.dumps(manifest, ensure_ascii=False))
    archive.write(checkpoint, 'model.pt')
    for path, record in zip(sorted(files), records):
        archive.write(path, record['path'])
state = Path('/www/nautilus/operator/state')
artifacts = ArtifactStore(state / 'artifacts')
store = ModelReleaseStore(state / 'models', artifacts)
active = store.active_id('rdt4quant_cpu_v2')
if active:
    result = store.get(active)
else:
    artifact = artifacts.register_file(bundle, kind='model', name='RDT4quant CPU',
                                       version='20260906-v2', filename=bundle.name)
    prepared = store.prepare('publish', {'artifactId': artifact['id']})
    receipt = store.execute('publish', prepared['request'], 'publish-rdt-cpu-20260906-v2', 'gabira')
    if receipt.get('receiptStatus') != 'completed':
        raise RuntimeError(str(receipt))
    result = store.get(store.active_id('rdt4quant_cpu_v2'))
output = {'releaseId': result['releaseId'], 'manifestSha256': result['manifestSha256'],
          'manifest': str(store.manifest_path(result['releaseId'])), 'modelHash': result['modelHash']}
Path('/tmp/montlok-cpu-release.json').write_text(json.dumps(output))
print(json.dumps(output))

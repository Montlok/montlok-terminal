import { describe, expect, it } from 'vitest';
import {
  nodeReadiness,
  normalizeRelease,
  selectedHeadSummary,
} from './releaseModel';

describe('published artifact evidence is distinct from device readiness', () => {
  it('unwraps the server manifest and interprets artifact-only validation', () => {
    const release = normalizeRelease({
      releaseId: 'rdt',
      status: 'validated',
      validation: 'artifact_valid',
      deployReady: false,
      deployReadiness: 'requires_target_node_preflight',
      manifest: {
        family: 'rdt4quant_multiasset',
        runtime: { device: 'cuda' },
        policy: { allowedModes: ['shadow'] },
      },
    });
    expect(release.validation.ok).toBe(true);
    expect(release.validation.scope).toBe('artifact_only');
    expect(release.runtime.device).toBe('cuda');
    expect(nodeReadiness(release)).toBe('请检查目标节点运行环境');
  });
  it('does not infer online CUDA from a device configuration', () => {
    expect(nodeReadiness({ runtime: { device: 'cuda' } })).toContain(
      '等待节点环境检查',
    );
    expect(
      nodeReadiness({
        nodeCompatibility: { ready: false, reason: '东京节点无 CUDA 资源' },
      }),
    ).toBe('东京节点无 CUDA 资源');
    expect(
      nodeReadiness({ nodeCompatibility: { ready: true, device: 'cpu' } }),
    ).toBe('当前节点可运行 · cpu');
  });
  it('uses explicit node rejection before the artifact-only preflight placeholder', () => {
    expect(
      nodeReadiness({
        deployReady: false,
        deployReadiness: 'requires_target_node_preflight',
        nodeCompatibility: {
          ready: false,
          reason: '目标节点没有可用 CUDA BF16',
          device: 'cuda',
          nodeId: 'local',
        },
      }),
    ).toBe('目标节点没有可用 CUDA BF16');
  });
  it('reads RDT selectedHeadByDomain as an integer index rather than a horizon or array', () => {
    const release = {
      outputContract: {
        selectedHeadByDomain: { crypto: 1, token_hour: 0 },
        horizons: { crypto: [15, 60], token_hour: [1, 4] },
        horizonUnit: { crypto: 'minutes', token_hour: 'hours' },
      },
    };
    expect(selectedHeadSummary(release, 'crypto')).toBe('索引 1 · 60 分钟');
    expect(selectedHeadSummary(release, 'token_hour')).toBe('索引 0 · 1 小时');
    expect(selectedHeadSummary(release, 'missing')).toBe('未配置有效预测头');
    expect(
      selectedHeadSummary(
        {
          outputContract: {
            ...release.outputContract,
            selectedHeadByDomain: { crypto: '1' },
          },
        },
        'crypto',
      ),
    ).toBe('未配置有效预测头');
  });
});

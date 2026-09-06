import type { Row } from '../../operator/api';

export function normalizeRelease(release: Row): Row {
  return {
    ...release.manifest,
    ...release,
    validation:
      release.validation === 'artifact_valid'
        ? {
            ok: true,
            checks: release.checks,
            errors: [],
            scope: 'artifact_only',
          }
        : release.validation,
  };
}

export function nodeReadiness(release: Row): string {
  const node = release.nodeCompatibility;
  if (node?.ready === true)
    return `当前节点可运行${node?.device ? ` · ${node.device}` : ''}`;
  if (node?.ready === false) return node.reason || '当前节点不可运行';
  if (release.deployReady === true) return '当前节点可运行';
  if (release.deployReadiness === 'requires_target_node_preflight')
    return '请检查目标节点运行环境';
  if (node?.ready === false || release.deployReady === false)
    return (
      node?.reason ||
      release.deployReason ||
      (release.runtime?.device?.startsWith('cuda')
        ? '需要当前节点提供可用 CUDA 资源'
        : '当前节点不可运行')
    );
  return '等待节点环境检查';
}

export function selectedHeadSummary(release: Row, domain: string): string {
  const output = release.outputContract;
  const head = output?.selectedHeadByDomain?.[domain];
  const horizons = output?.horizons?.[domain];
  if (
    !Number.isInteger(head) ||
    head < 0 ||
    !Array.isArray(horizons) ||
    horizons[head] === undefined
  )
    return '未配置有效预测头';
  const rawUnit =
    typeof output.horizonUnit === 'string'
      ? output.horizonUnit
      : output.horizonUnit?.[domain];
  const unit =
    ({ minutes: '分钟', hours: '小时', bars: '根' } as Record<string, string>)[
      rawUnit
    ] ||
    rawUnit ||
    '单位未提供';
  return `索引 ${head} · ${horizons[head]} ${unit}`;
}

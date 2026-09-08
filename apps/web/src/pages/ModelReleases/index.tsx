import { ReloadOutlined } from '@ant-design/icons';
import { Link, useModel } from '@umijs/max';
import { Alert, Button, Descriptions, Select, Space, Table } from 'antd';
import { useCallback, useRef, useState } from 'react';
import { api, type Row } from '../../operator/api';
import { ConfirmOperation } from '../../operator/ConfirmOperation';
import { ModelRuntime } from '../../operator/ModelRuntime';
import { usePoll } from '../../operator/usePoll';
import type { Artifact } from '../Artifacts/artifactModel';
import {
  nodeReadiness,
  normalizeRelease,
  selectedHeadSummary,
} from './releaseModel';
import '../Artifacts/artifacts.css';
import './models.css';

const STATUS: Record<string, string> = {
  validated: '已校验',
  published: '已发布',
  retired: '已退役',
  invalid: '校验失败',
};

export default function Models() {
  const { initialState } = useModel('@@initialState');
  const admin = initialState?.currentUser?.access === 'admin';
  const { selectedGroup, selectedRuns, setSelectedGroup } =
    useModel('operator');
  const [releases, setReleases] = useState<Row[]>([]);
  const [artifacts, setArtifacts] = useState<Artifact[]>([]);
  const [artifactId, setArtifactId] = useState(
    () => new URLSearchParams(window.location.search).get('artifactId') || '',
  );
  const [selected, setSelected] = useState('');
  const [ticket, setTicket] = useState<Row>();
  const [receipt, setReceipt] = useState<Row>();
  const [run, setRun] = useState<Row>();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const selection = `${selectedGroup}/${selectedRuns?.[selectedGroup] || ''}`;
  const currentSelection = useRef(selection);
  currentSelection.current = selection;
  const load = useCallback(async (signal?: AbortSignal) => {
    try {
      const [models, files] = await Promise.all([
        api('model-releases'),
        api('artifacts'),
      ]);
      if (signal?.aborted) return;
      const published: Row[] = (models.releases || []).map(normalizeRelease);
      setReleases((current) => [
        ...published,
        ...current.filter(
          (item) =>
            item.status === 'validated' &&
            !published.some((version) => version.releaseId === item.releaseId),
        ),
      ]);
      setArtifacts(
        (files.artifacts || []).filter(
          (item: Artifact) => item.kind === 'model',
        ),
      );
      setError('');
    } catch (reason) {
      if (!signal?.aborted) setError(String(reason));
    }
  }, []);
  usePoll(load, 10000);
  const loadRuntime = useCallback(
    async (signal?: AbortSignal) => {
      try {
        const result = await api(
          `strategy-groups/${encodeURIComponent(selectedGroup)}/runtime`,
        );
        if (signal?.aborted || currentSelection.current !== selection) return;
        const group = result.groups?.find(
          (item: Row) => item.groupId === selectedGroup,
        );
        const active = group?.runs?.find(
          (item: Row) => item.runId === selectedRuns?.[selectedGroup],
        );
        setRun(active ? { ...active, selection } : undefined);
      } catch {
        if (!signal?.aborted && currentSelection.current === selection)
          setRun(undefined);
      }
    },
    [selectedGroup, selectedRuns, selection],
  );
  usePoll(loadRuntime, 3000);
  const release = releases.find((item) => item.releaseId === selected);
  const domains = Object.entries(release?.domainContracts || {}).map(
    ([key, value]) => ({ key, ...(value as Row) }),
  );
  const artifact = artifacts.find((item) => item.id === artifactId);
  const actualRun = run?.selection === selection ? run : undefined;
  const receiptPending = ['unknown', 'processing'].includes(receipt?.status);
  const receiptFailed =
    ['error', 'failed'].includes(receipt?.status) ||
    receipt?.result?.validation?.ok === false;
  const validateReason = !admin
    ? '需要操作员权限'
    : busy
      ? '请求处理中'
      : !artifact
        ? '选择已登记的模型制品'
        : '';
  const publishReason = !admin
    ? '需要操作员权限'
    : busy
      ? '请求处理中'
      : !release
        ? '选择校验通过的版本'
        : release.status !== 'validated' ||
            release.validation?.ok !== true ||
            !release.artifactId ||
            !release.manifestSha256
          ? '请选择校验通过的待发布版本'
          : '';
  async function prepare(action: 'validate' | 'publish') {
    if (action === 'validate' ? validateReason : publishReason) return;
    setBusy(true);
    setError('');
    try {
      setTicket(
        await api('prepare', {
          kind: 'model_release',
          name: action,
          arguments:
            action === 'validate'
              ? { artifactId }
              : {
                  artifactId: release?.artifactId,
                  manifestSha256: release?.manifestSha256,
                },
        }),
      );
    } catch (reason) {
      setError(String(reason));
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="management-page artifacts-page models-page">
      <div className="page-heading">
        <div>
          <h1>模型发布</h1>
          <p>模型制品、发布版本与运行观测</p>
        </div>
        <Button icon={<ReloadOutlined />} onClick={() => void load()}>
          刷新
        </Button>
      </div>
      {error && <Alert type="error" title={error} showIcon />}
      <Alert
        type="info"
        title="模型发布流程"
        description="选择已登记制品，校验文件与配置后发布。在策略组设置预算和时长，确认后启动运行。"
      />
      <section className="panel artifact-uploader" aria-label="校验模型制品">
        <div className="panel-heading">
          <strong>校验已登记模型</strong>
          <Link to="/strategies/resources/artifacts">上传模型制品</Link>
        </div>
        <div className="artifact-fields">
          <Select
            aria-label="待校验模型制品"
            value={artifactId || undefined}
            placeholder="选择已登记模型"
            disabled={busy}
            onChange={setArtifactId}
            options={artifacts.map((item) => ({
              value: item.id,
              label: `${item.name} · ${item.version} · ${item.filename}`,
            }))}
          />
          <div>
            <Button
              disabled={!!validateReason}
              loading={busy}
              aria-describedby={
                validateReason ? 'model-validate-reason' : undefined
              }
              onClick={() => void prepare('validate')}
            >
              校验模型
            </Button>
            {validateReason && (
              <p id="model-validate-reason" className="artifact-hint">
                {validateReason}
              </p>
            )}
          </div>
        </div>
      </section>
      {receipt && (
        <Alert
          type={receiptPending ? 'warning' : receiptFailed ? 'error' : 'info'}
          title={
            receiptPending
              ? '正在核对模型操作结果，请刷新版本列表'
              : receiptFailed
                ? `${receipt.action === 'publish' ? '模型发布' : '模型校验'}失败`
                : receipt.action === 'publish'
                  ? '模型版本已发布'
                  : '模型校验完成'
          }
          description={
            <details>
              <summary>校验或发布回执</summary>
              <pre className="json-output">
                {JSON.stringify(receipt.result, null, 2)}
              </pre>
            </details>
          }
        />
      )}
      <Table<Row>
        className="panel"
        rowKey="releaseId"
        size="small"
        dataSource={releases}
        pagination={{ pageSize: 10, showSizeChanger: false }}
        scroll={{ x: 850 }}
        locale={{ emptyText: '上传并校验模型后，版本将显示在这里' }}
        columns={[
          { title: '模型版本', dataIndex: 'modelVersion' },
          { title: '运行器', dataIndex: 'runnerId' },
          { title: '模型类型', dataIndex: 'family' },
          {
            title: '发布状态',
            dataIndex: 'status',
            render: (value: string) => STATUS[value] || value,
          },
          {
            title: '当前节点',
            key: 'node',
            render: (_, item) => nodeReadiness(item),
          },
          {
            title: '清单摘要',
            dataIndex: 'manifestSha256',
            render: (value: string) => (
              <span title={value}>{value?.slice(0, 12) || '读取中'}</span>
            ),
          },
          {
            title: '查看',
            key: 'view',
            render: (_, item) => (
              <Button type="link" onClick={() => setSelected(item.releaseId)}>
                版本详情
              </Button>
            ),
          },
        ]}
      />
      {release && (
        <section className="panel artifact-uploader" aria-label="模型版本详情">
          <div className="panel-heading">
            <strong>
              {release.modelVersion} ·{' '}
              {STATUS[release.status] || release.status}
            </strong>
            <div>
              <Button
                type="primary"
                disabled={!!publishReason}
                aria-describedby={
                  publishReason ? 'model-publish-reason' : undefined
                }
                onClick={() => void prepare('publish')}
              >
                发布此版本
              </Button>
              {publishReason && (
                <p id="model-publish-reason" className="artifact-hint">
                  {publishReason}
                </p>
              )}
            </div>
          </div>
          <Descriptions
            size="small"
            column={1}
            items={[
              {
                key: 'node',
                label: '当前节点可运行性',
                children: nodeReadiness(release),
              },
              {
                key: 'research',
                label: '研究来源 / 训练版本',
                children:
                  release.provenance?.trainingRunId ||
                  release.provenance?.trainingVersion ||
                  release.provenance?.source ||
                  '请展开来源记录查看',
              },
              {
                key: 'shadow',
                label: '运行限制',
                children:
                  release.policy?.shadowOnly === true ||
                  release.policy?.executionMode === 'shadow' ||
                  release.policy?.allowedModes?.every((mode: string) =>
                    mode.includes('shadow'),
                  )
                    ? '研究评估'
                    : '按发布配置运行',
              },
              { key: 'id', label: '模型发布编号', children: release.releaseId },
              {
                key: 'device',
                label: '配置设备',
                children: release.runtime?.device || '等待发布信息',
              },
              {
                key: 'features',
                label: '输入特征',
                children: domains.length
                  ? '按数据域与品种固定，见下表'
                  : (release.featureContract?.names || []).join('、') ||
                    '等待发布信息',
              },
              {
                key: 'window',
                label: '特征窗口',
                children: domains.length
                  ? `${domains.length} 个分域输入约定，见下表`
                  : `${release.featureContract?.sequenceBars ?? '读取中'} 根 · ${release.featureContract?.barSeconds ?? '读取中'} 秒 / 根`,
              },
              {
                key: 'markets',
                label: '所需市场',
                children: domains.length
                  ? `${domains.length} 个分域品种（见下表）`
                  : (release.featureContract?.requiredMarkets || []).join(
                      '、',
                    ) || '等待发布信息',
              },
              {
                key: 'modes',
                label: '允许运行环境',
                children:
                  (release.policy?.allowedModes || [])
                    .map((mode: string) =>
                      mode === 'nautilus_sandbox' || mode === 'sandbox'
                        ? '研究回放'
                        : mode === 'live'
                          ? '实盘'
                          : mode === 'shadow'
                            ? '研究评估'
                            : mode,
                    )
                    .join('、') || '等待发布信息',
              },
              {
                key: 'validation',
                label: '校验结果',
                children:
                  release.validation?.ok === true
                    ? '文件与配置校验通过'
                    : release.validation?.ok === false
                      ? '未通过'
                      : '等待校验',
              },
            ]}
          />
          {!!domains.length && (
            <Table<Row>
              size="small"
              rowKey="key"
              dataSource={domains}
              pagination={{ pageSize: 5, showSizeChanger: false }}
              scroll={{ x: 700 }}
              columns={[
                { title: '数据域', dataIndex: 'domain' },
                { title: '品种', dataIndex: 'instrument' },
                {
                  title: '输入特征（顺序固定）',
                  dataIndex: 'names',
                  render: (names: string[]) =>
                    names?.join('、') || '等待发布信息',
                },
                { title: '窗口 / 根', dataIndex: 'sequenceBars' },
                { title: '周期 / 秒', dataIndex: 'barSeconds' },
                {
                  title: '选用预测头（零起始索引）',
                  key: 'selectedHead',
                  render: (_, contract) =>
                    selectedHeadSummary(release, contract.domain),
                },
              ]}
            />
          )}
          {!!release.validation?.errors?.length && (
            <Alert
              type="error"
              title="模型校验未通过"
              description={release.validation.errors.join('；')}
            />
          )}
          <details>
            <summary>完整输入输出、校验检查与来源版本</summary>
            <pre className="json-output">
              {JSON.stringify(release, null, 2)}
            </pre>
          </details>
          <Space>
            <Button
              type="primary"
              disabled={!release.groupId || release.deployReady !== true}
              onClick={() => setSelectedGroup(release.groupId)}
            >
              选择此模型
            </Button>
            <Link to="/strategies/groups/overview">查看策略组与运行实例</Link>
          </Space>
        </section>
      )}
      <section
        className="panel artifact-uploader"
        aria-label="当前选择实例的模型状态"
      >
        <div className="panel-heading">
          <strong>当前选择实例 · {actualRun?.runId || '未选择有效实例'}</strong>
          <span>运行服务观测</span>
        </div>
        <ModelRuntime model={actualRun?.engine?.model ?? actualRun?.model} />
      </section>
      <ConfirmOperation
        ticket={ticket}
        onClose={() => setTicket(undefined)}
        onComplete={(result) => {
          setReceipt({ ...result, action: ticket?.operation?.name });
          const validated = result.result?.release || result.result;
          if (
            ticket?.operation?.name === 'validate' &&
            result.status === 'completed' &&
            validated?.releaseId
          ) {
            const version = normalizeRelease(validated);
            setReleases((current) => [
              ...current.filter((item) => item.releaseId !== version.releaseId),
              version,
            ]);
            setSelected(version.releaseId);
          }
          void load();
        }}
      />
    </div>
  );
}

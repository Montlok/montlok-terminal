import { ReloadOutlined, UploadOutlined } from '@ant-design/icons';
import { Link, useModel } from '@umijs/max';
import { Alert, Button, Input, Select, Table, Upload } from 'antd';
import { useCallback, useState } from 'react';
import { api, number } from '../../operator/api';
import { usePoll } from '../../operator/usePoll';
import {
  type Artifact,
  type ArtifactKind,
  acceptedFiles,
  kindLabels,
  uploadArtifact,
  validateUpload,
} from './artifactModel';
import './artifacts.css';

export default function Artifacts() {
  const { initialState } = useModel('@@initialState');
  const canUpload = initialState?.currentUser?.access === 'admin';
  const [artifacts, setArtifacts] = useState<Artifact[]>([]);
  const [kind, setKind] = useState<ArtifactKind>('factor');
  const [name, setName] = useState('');
  const [version, setVersion] = useState('');
  const [file, setFile] = useState<File>();
  const [filter, setFilter] = useState('all');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [receipt, setReceipt] = useState<Artifact>();
  const load = useCallback(async (signal?: AbortSignal) => {
    try {
      const result = await api<{ artifacts: Artifact[] }>('artifacts');
      if (!signal?.aborted) setArtifacts(result.artifacts);
    } catch (reason) {
      if (!signal?.aborted)
        setError(reason instanceof Error ? reason.message : String(reason));
    }
  }, []);
  usePoll(load, 15000);
  async function upload() {
    if (!file || !name.trim() || !version.trim()) {
      setError('请填写名称、版本并选择文件');
      return;
    }
    setBusy(true);
    setError('');
    setReceipt(undefined);
    try {
      const result = await uploadArtifact(
        { kind, name: name.trim(), version: version.trim() },
        file,
      );
      setReceipt(result);
      setFile(undefined);
      await load();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : String(reason));
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="management-page artifacts-page">
      <div className="page-heading">
        <div>
          <h1>文件与版本</h1>
          <p>因子定义、模型文件与策略配置</p>
          <Link to="/strategies/resources/models">模型校验与发布</Link>
        </div>
        <Button icon={<ReloadOutlined />} onClick={() => void load()}>
          刷新
        </Button>
      </div>
      {error && <Alert type="error" title={error} showIcon />}
      {receipt && (
        <Alert
          type="success"
          title={`${receipt.name} · ${receipt.version} 已登记`}
          description="下一步：校验文件与配置"
          showIcon
        />
      )}
      {canUpload ? (
        <section className="panel artifact-uploader">
          <div className="panel-heading">
            <strong>上传文件</strong>
            <span>64 MiB / 文件 · JSON 4 MiB</span>
          </div>
          <div className="artifact-fields">
            <label htmlFor="artifact-kind">
              类型
              <Select
                id="artifact-kind"
                aria-label="文件类型"
                value={kind}
                disabled={busy}
                onChange={(value) => {
                  setKind(value);
                  setFile(undefined);
                }}
                options={Object.entries(kindLabels).map(([value, label]) => ({
                  value,
                  label,
                }))}
              />
            </label>
            <label htmlFor="artifact-name">
              名称
              <Input
                id="artifact-name"
                aria-label="文件名称"
                value={name}
                maxLength={80}
                disabled={busy}
                onChange={(event) => setName(event.target.value)}
                placeholder="例如 sector-momentum"
              />
            </label>
            <label htmlFor="artifact-version">
              版本
              <Input
                id="artifact-version"
                aria-label="文件版本"
                value={version}
                maxLength={64}
                disabled={busy}
                onChange={(event) => setVersion(event.target.value)}
                placeholder="例如 1.0.0"
              />
            </label>
            <div className="artifact-file-control">
              <Upload
                accept={acceptedFiles[kind]}
                multiple={false}
                maxCount={1}
                disabled={busy}
                fileList={
                  file
                    ? [{ uid: 'selected', name: file.name, status: 'done' }]
                    : []
                }
                beforeUpload={(selected) => {
                  const issue = validateUpload(selected, kind);
                  if (issue) {
                    setError(issue);
                    return Upload.LIST_IGNORE;
                  }
                  setError('');
                  setFile(selected);
                  return false;
                }}
                onRemove={() => {
                  setFile(undefined);
                  return true;
                }}
                onChange={({ fileList }) => {
                  if (!fileList.length) setFile(undefined);
                }}
              >
                <Button icon={<UploadOutlined />} disabled={busy}>
                  选择文件
                </Button>
              </Upload>
            </div>
            <Button
              type="primary"
              disabled={!file || !name.trim() || !version.trim()}
              loading={busy}
              onClick={() => void upload()}
            >
              上传并登记
            </Button>
          </div>
          <p className="artifact-hint">
            {kind === 'model' ? 'ONNX / safetensors / ZIP' : 'JSON / ZIP'} ·
            版本内容固定；更新文件请使用新版本号
          </p>
        </section>
      ) : (
        <p className="artifact-hint">已登记版本可浏览与查询</p>
      )}
      <div className="artifact-list-heading">
        <strong>已登记版本</strong>
        <Select
          aria-label="筛选文件类型"
          value={filter}
          onChange={setFilter}
          options={[
            { value: 'all', label: '全部类型' },
            ...Object.entries(kindLabels).map(([value, label]) => ({
              value,
              label,
            })),
          ]}
        />
      </div>
      <Table<Artifact>
        rowKey="id"
        size="small"
        className="panel"
        pagination={{ pageSize: 15, showSizeChanger: false }}
        dataSource={artifacts.filter(
          (item) => filter === 'all' || item.kind === filter,
        )}
        scroll={{ x: 900 }}
        columns={[
          {
            title: '发布入口',
            key: 'release',
            width: 150,
            render: (_, artifact) =>
              artifact.kind === 'model' ? (
                <Link
                  to={`/strategies/resources/models?artifactId=${encodeURIComponent(artifact.id)}`}
                >
                  校验 / 发布模型
                </Link>
              ) : (
                '文件登记'
              ),
          },
          {
            title: '类型',
            dataIndex: 'kind',
            render: (value: ArtifactKind) => kindLabels[value],
            width: 150,
          },
          { title: '名称', dataIndex: 'name' },
          { title: '版本', dataIndex: 'version', width: 120 },
          { title: '文件', dataIndex: 'filename' },
          {
            title: '大小',
            dataIndex: 'bytes',
            render: (value: number) => `${number(value / 1024, 1)} KiB`,
            width: 110,
          },
          {
            title: '状态',
            key: 'state',
            render: () => '已登记 · 待验证',
            width: 150,
          },
          {
            title: '登记时间',
            dataIndex: 'createdAt',
            render: (value: number) =>
              new Date(value * 1000).toLocaleString('zh-CN', {
                timeZone: 'Asia/Shanghai',
                hour12: false,
              }),
            width: 190,
          },
        ]}
        expandable={{
          expandedRowRender: (item) => (
            <dl className="artifact-detail">
              <dt>登记 ID</dt>
              <dd>{item.id}</dd>
              <dt>SHA-256</dt>
              <dd>{item.sha256}</dd>
              <dt>文件格式</dt>
              <dd>{item.format}</dd>
              <dt>容器检查</dt>
              <dd>{JSON.stringify(item.checks)}</dd>
            </dl>
          ),
        }}
      />
    </div>
  );
}

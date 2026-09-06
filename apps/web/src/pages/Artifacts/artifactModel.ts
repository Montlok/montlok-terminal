import { ensureSession } from '../../operator/api';

export type ArtifactKind = 'factor' | 'model' | 'strategy';
export type Artifact = {
  id: string;
  kind: ArtifactKind;
  name: string;
  version: string;
  filename: string;
  sha256: string;
  bytes: number;
  format: string;
  createdAt: number;
  status: 'registered';
  validation: 'pending_validation';
  checks: Record<string, unknown>;
};

export const kindLabels: Record<ArtifactKind, string> = {
  factor: '因子定义',
  model: '模型文件',
  strategy: '策略配置 / 包',
};

export const acceptedFiles: Record<ArtifactKind, string> = {
  factor: '.json,.zip',
  model: '.onnx,.safetensors,.zip',
  strategy: '.json,.zip',
};

export function validateUpload(
  file: File,
  kind: ArtifactKind,
): string | undefined {
  if (!file.size) return '文件不能为空';
  if (file.size > 64 * 1024 * 1024) return '单文件不能超过 64 MiB';
  const extension = `.${file.name.split('.').at(-1)?.toLowerCase()}`;
  if (!acceptedFiles[kind].split(',').includes(extension))
    return '文件格式与所选类型不匹配';
  if (extension === '.json' && file.size > 4 * 1024 * 1024)
    return 'JSON 定义不能超过 4 MiB';
  return undefined;
}

export async function uploadArtifact(
  fields: { kind: ArtifactKind; name: string; version: string },
  file: File,
): Promise<Artifact> {
  const error = validateUpload(file, fields.kind);
  if (error) throw new Error(error);
  const identity = await ensureSession();
  if (identity.role !== 'admin') throw new Error('此账户仅可查看文件');
  const form = new FormData();
  form.append('kind', fields.kind);
  form.append('name', fields.name);
  form.append('version', fields.version);
  form.append('file', file);
  const response = await fetch('/api/artifacts', {
    method: 'POST',
    credentials: 'same-origin',
    headers: { 'X-Operator-CSRF': identity.csrf },
    body: form,
  });
  const text = await response.text();
  let value: { artifact?: Artifact; error?: string; code?: string };
  try {
    value = JSON.parse(text);
  } catch {
    throw new Error(`上传失败（HTTP ${response.status}）`);
  }
  if (!response.ok) {
    if (response.status === 401) window.location.replace('/login');
    if (value.code === 'SESSION_CSRF_MISMATCH')
      throw new Error('会话已更新，请刷新页面后重新上传');
    throw new Error(value.error || `上传失败（HTTP ${response.status}）`);
  }
  if (!value.artifact) throw new Error('上传服务未返回登记记录');
  return value.artifact;
}

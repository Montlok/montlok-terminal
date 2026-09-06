import { afterEach, describe, expect, it, vi } from 'vitest';
import { ensureSession } from '../../operator/api';
import { uploadArtifact, validateUpload } from './artifactModel';

vi.mock('../../operator/api', () => ({ ensureSession: vi.fn() }));
afterEach(() => {
  vi.clearAllMocks();
  vi.unstubAllGlobals();
});

describe('artifact uploads', () => {
  it('checks type-specific formats and bounded sizes before sending', () => {
    expect(
      validateUpload(new File(['{}'], 'factor.json'), 'factor'),
    ).toBeUndefined();
    expect(
      validateUpload(new File(['model'], 'model.onnx'), 'model'),
    ).toBeUndefined();
    expect(
      validateUpload(new File(['pickle'], 'model.pkl'), 'model'),
    ).toContain('格式');
    expect(validateUpload(new File(['{}'], 'model.json'), 'model')).toContain(
      '格式',
    );
    expect(validateUpload(new File([], 'empty.json'), 'factor')).toBe(
      '文件不能为空',
    );
    const large = new File(['data'], 'model.onnx');
    Object.defineProperty(large, 'size', { value: 65 * 1024 * 1024 });
    expect(validateUpload(large, 'model')).toContain('64 MiB');
  });

  it('sends real multipart with session CSRF and preserves browser boundary generation', async () => {
    vi.mocked(ensureSession).mockResolvedValue({
      operator: 'test',
      role: 'admin',
      csrf: 'csrf-test',
      mode: 'demo',
    });
    const fetch = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          artifact: { id: 'registered', validation: 'pending_validation' },
        }),
        { status: 200 },
      ),
    );
    vi.stubGlobal('fetch', fetch);
    const file = new File(['{}'], 'factor.json');
    const result = await uploadArtifact(
      { kind: 'factor', name: 'momentum', version: '1.0' },
      file,
    );
    expect(result.validation).toBe('pending_validation');
    expect(fetch).toHaveBeenCalledTimes(1);
    const [path, request] = fetch.mock.calls[0];
    expect(path).toBe('/api/artifacts');
    expect(request.headers).toEqual({ 'X-Operator-CSRF': 'csrf-test' });
    expect(request.body).toBeInstanceOf(FormData);
    expect(request.body.get('kind')).toBe('factor');
    expect(request.body.get('version')).toBe('1.0');
    expect(request.body.get('file').name).toBe('factor.json');
  });

  it('viewer cannot upload and expired write sessions are not automatically replayed', async () => {
    vi.mocked(ensureSession).mockResolvedValue({
      operator: 'test',
      role: 'viewer',
      csrf: 'csrf-test',
      mode: 'demo',
    });
    const fetch = vi.fn();
    vi.stubGlobal('fetch', fetch);
    const fields = {
      kind: 'factor' as const,
      name: 'momentum',
      version: '1.0',
    };
    await expect(
      uploadArtifact(fields, new File(['{}'], 'factor.json')),
    ).rejects.toThrow('仅可查看');
    expect(fetch).not.toHaveBeenCalled();
    vi.mocked(ensureSession).mockResolvedValue({
      operator: 'test',
      role: 'admin',
      csrf: 'csrf-test',
      mode: 'demo',
    });
    fetch.mockResolvedValue(
      new Response(JSON.stringify({ code: 'SESSION_CSRF_MISMATCH' }), {
        status: 403,
      }),
    );
    await expect(
      uploadArtifact(fields, new File(['{}'], 'factor.json')),
    ).rejects.toThrow('会话已更新');
    expect(fetch).toHaveBeenCalledTimes(1);
  });
});

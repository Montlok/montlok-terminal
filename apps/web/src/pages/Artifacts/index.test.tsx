import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';
import type { ReactNode } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { api } from '../../operator/api';
import { type Artifact, uploadArtifact } from './artifactModel';
import Artifacts from './index';

const identity = vi.hoisted(() => ({ access: 'admin' }));
vi.mock('@umijs/max', () => ({
  useModel: () => ({ initialState: { currentUser: identity } }),
  Link: ({ to, children }: { to: string; children: ReactNode }) => (
    <a href={to}>{children}</a>
  ),
}));
vi.mock('../../operator/api', async (load) => ({
  ...(await load<typeof import('../../operator/api')>()),
  api: vi.fn(),
}));
vi.mock('./artifactModel', async (load) => ({
  ...(await load<typeof import('./artifactModel')>()),
  uploadArtifact: vi.fn(),
}));

const item: Artifact = {
  id: 'abc',
  kind: 'factor',
  name: 'momentum',
  version: '1.0',
  filename: 'factor.json',
  sha256: 'a'.repeat(64),
  bytes: 128,
  format: 'json',
  createdAt: 1788600000,
  status: 'registered',
  validation: 'pending_validation',
  checks: { container: 'json' },
};

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
  identity.access = 'admin';
});

describe('artifact registry page', () => {
  it('uploads selected file with explicit metadata and shows registered-not-active receipt', async () => {
    vi.mocked(api).mockResolvedValue({ artifacts: [item] });
    vi.mocked(uploadArtifact).mockResolvedValue(item);
    const { container } = render(<Artifacts />);
    expect(await screen.findByText('momentum')).toBeInTheDocument();
    fireEvent.change(screen.getByRole('textbox', { name: '文件名称' }), {
      target: { value: 'momentum' },
    });
    fireEvent.change(screen.getByRole('textbox', { name: '文件版本' }), {
      target: { value: '1.0' },
    });
    const file = new File(['{}'], 'factor.json');
    fireEvent.change(
      container.querySelector('input[type="file"]') as HTMLInputElement,
      { target: { files: [file] } },
    );
    await waitFor(() =>
      expect(
        screen.getByRole('button', { name: '上传并登记' }),
      ).not.toBeDisabled(),
    );
    fireEvent.click(screen.getByRole('button', { name: '上传并登记' }));
    expect(
      await screen.findByText('momentum · 1.0 已登记'),
    ).toBeInTheDocument();
    expect(uploadArtifact).toHaveBeenCalledWith(
      { kind: 'factor', name: 'momentum', version: '1.0' },
      expect.objectContaining({ name: 'factor.json' }),
    );
    expect(screen.getByText('下一步：校验文件与配置')).toBeInTheDocument();
    expect(
      screen.queryByRole('button', { name: /开始交易|启动模型/ }),
    ).not.toBeInTheDocument();
  });

  it('lets viewer inspect versions without exposing upload action', async () => {
    identity.access = 'viewer';
    vi.mocked(api).mockResolvedValue({ artifacts: [item] });
    render(<Artifacts />);
    expect(await screen.findByText('momentum')).toBeInTheDocument();
    expect(screen.getByText('已登记 · 待验证')).toBeInTheDocument();
    expect(
      screen.queryByRole('button', { name: '上传并登记' }),
    ).not.toBeInTheDocument();
  });
});

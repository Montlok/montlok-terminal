import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const identity = (csrf: string) => ({
  csrf,
  operator: 'gabira',
  role: 'admin',
  mode: 'demo',
});
const reply = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status });
const mismatch = () =>
  reply({ code: 'SESSION_CSRF_MISMATCH', error: '操作会话已更新' }, 403);

describe('operator session recovery', () => {
  const fetchMock = vi.fn();
  beforeEach(() => {
    vi.resetModules();
    fetchMock.mockReset();
    vi.stubGlobal('fetch', fetchMock);
  });
  afterEach(() => vi.unstubAllGlobals());

  it('shares concurrent reads but refreshes again after the response', async () => {
    fetchMock
      .mockResolvedValueOnce(reply(identity('same')))
      .mockResolvedValueOnce(reply({ groups: [] }))
      .mockResolvedValueOnce(reply({ groups: ['new'] }));
    const { api } = await import('./api');
    const results = await Promise.all([
      api('strategy-groups'),
      api('strategy-groups'),
      api('strategy-groups'),
    ]);
    expect(results).toEqual([{ groups: [] }, { groups: [] }, { groups: [] }]);
    expect(
      fetchMock.mock.calls.filter(([url]) => url === '/api/strategy-groups'),
    ).toHaveLength(1);
    expect(await api('strategy-groups')).toEqual({ groups: ['new'] });
  });

  it('refreshes a changed session and retries a read-only query once', async () => {
    fetchMock
      .mockResolvedValueOnce(reply(identity('old')))
      .mockResolvedValueOnce(mismatch())
      .mockResolvedValueOnce(reply(identity('new')))
      .mockResolvedValueOnce(reply({ result: [1] }));
    const { api } = await import('./api');
    expect(
      await api('query', { kind: 'rest', name: 'GET /api/v5/market/ticker' }),
    ).toEqual({ result: [1] });
    expect(fetchMock.mock.calls[1][1].headers['X-Operator-CSRF']).toBe('old');
    expect(fetchMock.mock.calls[3][1].headers['X-Operator-CSRF']).toBe('new');
    expect(fetchMock).toHaveBeenCalledTimes(4);
  });

  it('never automatically replays a confirmed mutation', async () => {
    fetchMock
      .mockResolvedValueOnce(reply(identity('old')))
      .mockResolvedValueOnce(mismatch())
      .mockResolvedValueOnce(reply(identity('new')));
    const { api } = await import('./api');
    await expect(api('execute', { id: 'ticket' })).rejects.toThrow(
      '请重新确认操作',
    );
    expect(
      fetchMock.mock.calls.filter(([url]) => url === '/api/execute'),
    ).toHaveLength(1);
  });

  it('does not treat an ordinary forbidden response as a stale session', async () => {
    fetchMock
      .mockResolvedValueOnce(reply(identity('old')))
      .mockResolvedValueOnce(reply({ error: '只读权限' }, 403));
    const { api } = await import('./api');
    await expect(api('query', {})).rejects.toThrow('只读权限');
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it('reloads the application instead of reusing permissions after an identity change', async () => {
    const reload = vi
      .spyOn(window.location, 'reload')
      .mockImplementation(() => {});
    fetchMock
      .mockResolvedValueOnce(reply(identity('old')))
      .mockResolvedValueOnce(mismatch())
      .mockResolvedValueOnce(
        reply({ ...identity('new'), operator: 'audit', role: 'viewer' }),
      );
    const { api } = await import('./api');
    await expect(api('query', {})).rejects.toThrow('登录身份已变更');
    expect(reload).toHaveBeenCalledOnce();
    expect(fetchMock).toHaveBeenCalledTimes(3);
    reload.mockRestore();
  });

  it('bounds recovery when the session changes again during a retry', async () => {
    fetchMock
      .mockResolvedValueOnce(reply(identity('old')))
      .mockResolvedValueOnce(mismatch())
      .mockResolvedValueOnce(reply(identity('new')))
      .mockResolvedValueOnce(mismatch());
    const { api } = await import('./api');
    await expect(api('query', {})).rejects.toThrow('操作会话已更新');
    expect(fetchMock).toHaveBeenCalledTimes(4);
  });

  it('coalesces initial session requests', async () => {
    fetchMock.mockResolvedValue(reply(identity('same')));
    const { ensureSession } = await import('./api');
    await Promise.all([ensureSession(), ensureSession(), ensureSession()]);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
  it('returns the operation identity for an uncertain write without replaying', async () => {
    fetchMock
      .mockResolvedValueOnce(reply(identity('same')))
      .mockRejectedValueOnce(new DOMException('deadline', 'TimeoutError'));
    const { api } = await import('./api');
    expect(await api('execute', { id: 'operation-123' })).toMatchObject({
      id: 'operation-123',
      status: 'unknown',
      result: { receiptStatus: 'unknown' },
    });
    expect(
      fetchMock.mock.calls.filter(([url]) => url === '/api/execute'),
    ).toHaveLength(1);
  });

  it('formats OKX millisecond strings and epoch seconds in explicit UTC+8', async () => {
    const { timeOf } = await import('./api');
    expect(timeOf('1788624000000')).toBe('00:00:00');
    expect(timeOf(1788624000)).toBe('00:00:00');
    expect(timeOf('2026-09-05T16:00:00Z')).toBe('00:00:00');
    expect(timeOf('not-a-time')).toBe('—');
  });
});

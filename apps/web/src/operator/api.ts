export type Row = Record<string, any>;
export type Operation = {
  kind: 'mcp' | 'rest' | 'native' | 'profile' | 'group' | 'model_release';
  name: string;
  arguments: Row | Row[];
};
export type SessionInfo = {
  csrf: string;
  operator: string;
  role: 'admin' | 'viewer';
  mode: string;
};
let session: SessionInfo | undefined;
let starting: Promise<SessionInfo> | undefined;
export function ensureSession(): Promise<SessionInfo> {
  if (starting) return starting;
  if (session) return Promise.resolve(session);
  starting = fetch('/api/session', {
    credentials: 'same-origin',
    cache: 'no-store',
  })
    .then(async (response) => {
      if (response.status === 401) {
        if (window.location.pathname !== '/login')
          window.location.replace('/login');
        throw new Error('请登录交易操作台');
      }
      if (!response.ok) throw new Error('无法连接操作服务');
      const value: SessionInfo = await response.json();
      if (
        !value.csrf ||
        !value.operator ||
        !['admin', 'viewer'].includes(value.role)
      )
        throw new Error('登录会话无效，请重新登录');
      session = value;
      return value;
    })
    .finally(() => {
      starting = undefined;
    });
  return starting;
}
export async function api<T = Row>(path: string, body?: unknown): Promise<T> {
  return request<T>(path, body, false);
}
async function request<T>(
  path: string,
  body: unknown,
  retried: boolean,
): Promise<T> {
  const identity = await ensureSession();
  const response = await fetch(`/api/${path}`, {
    method: body === undefined ? 'GET' : 'POST',
    credentials: 'same-origin',
    headers: {
      'Content-Type': 'application/json',
      'X-Operator-CSRF': identity.csrf,
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await response.text();
  if (response.status === 401) {
    session = undefined;
    starting = undefined;
    window.location.replace('/login');
    throw new Error('登录已过期');
  }
  let value: Row;
  try {
    value = JSON.parse(text);
  } catch {
    throw new Error(text || `HTTP ${response.status}`);
  }
  if (
    response.status === 403 &&
    value.code === 'SESSION_CSRF_MISMATCH' &&
    !retried
  ) {
    if (session?.csrf === identity.csrf) session = undefined;
    const refreshed = await ensureSession();
    if (
      refreshed.operator !== identity.operator ||
      refreshed.role !== identity.role
    ) {
      window.location.reload();
      throw new Error('登录身份已变更，正在刷新');
    }
    // Only reads may be replayed. Prepared/confirmed actions remain under user control.
    if (body === undefined || path === 'query')
      return request<T>(path, body, true);
    throw new Error('会话已更新，请重新确认操作');
  }
  if (!response.ok) throw new Error(value.error || `HTTP ${response.status}`);
  return value as T;
}
export function dataOf(value: any): any {
  if (value?.result !== undefined) return dataOf(value.result);
  const text = value?.content?.find((item: Row) => item.type === 'text')?.text;
  if (text) {
    try {
      return dataOf(JSON.parse(text));
    } catch {
      return text;
    }
  }
  if (value?.data !== undefined) return dataOf(value.data);
  return value;
}
export function number(value: unknown, digits = 2): string {
  if (value === undefined || value === null || value === '') return '—';
  const numeric = Number(value);
  return Number.isFinite(numeric)
    ? numeric.toLocaleString('en-US', { maximumFractionDigits: digits })
    : '—';
}
export function timeOf(value: unknown): string {
  if (!value) return '—';
  const numeric =
    typeof value === 'number'
      ? value
      : typeof value === 'string' && /^\d+(?:\.\d+)?$/.test(value)
        ? Number(value)
        : undefined;
  const timestamp =
    numeric === undefined ? value : numeric < 1e12 ? numeric * 1000 : numeric;
  const date = new Date(timestamp as string | number);
  if (!Number.isFinite(date.getTime())) return '—';
  return date.toLocaleTimeString('zh-CN', {
    hour12: false,
    timeZone: 'Asia/Shanghai',
  });
}

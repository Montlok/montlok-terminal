export type Row = Record<string, any>;
export type Operation = {
  kind: 'mcp' | 'rest' | 'native' | 'profile';
  name: string;
  arguments: Row | Row[];
};
let csrf = '';
let starting: Promise<void> | undefined;
export function ensureSession(): Promise<void> {
  if (csrf) return Promise.resolve();
  if (!starting)
    starting = fetch('/api/session', { credentials: 'same-origin' })
      .then(async (response) => {
        if (response.status === 401) {
          if (window.location.pathname !== '/login')
            window.location.replace('/login');
          throw new Error('请登录交易操作台');
        }
        if (!response.ok) throw new Error('无法连接操作服务');
        csrf = (await response.json()).csrf;
      })
      .catch((error) => {
        starting = undefined;
        throw error;
      });
  return starting;
}
export async function api<T = Row>(path: string, body?: unknown): Promise<T> {
  await ensureSession();
  const response = await fetch(`/api/${path}`, {
    method: body === undefined ? 'GET' : 'POST',
    credentials: 'same-origin',
    headers: { 'Content-Type': 'application/json', 'X-Operator-CSRF': csrf },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await response.text();
  if (response.status === 401) {
    csrf = '';
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
  const timestamp =
    typeof value === 'number' && value < 1e12 ? value * 1000 : value;
  return new Date(timestamp as string | number).toLocaleTimeString('zh-CN', {
    hour12: false,
  });
}

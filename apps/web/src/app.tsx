import type { RequestConfig } from '@umijs/max';
import dayjs from 'dayjs';
import relativeTime from 'dayjs/plugin/relativeTime';
import { ensureSession, type SessionInfo } from './operator/api';
import './operator/theme.css';
dayjs.extend(relativeTime);
export async function getInitialState(): Promise<{
  currentUser?: API.CurrentUser;
  fetchUserInfo?: () => Promise<API.CurrentUser | undefined>;
  settings?: { layout: string };
  settingDrawerOpen?: boolean;
}> {
  if (window.location.pathname === '/login') return {};
  let session: SessionInfo;
  try {
    session = await ensureSession();
  } catch {
    return {};
  }
  const currentUser: API.CurrentUser = {
    name: session.operator,
    userid: session.operator,
    access: session.role,
  };
  return {
    currentUser,
    fetchUserInfo: async () => currentUser,
    settings: { layout: 'terminal' },
    settingDrawerOpen: false,
  };
}
export const request: RequestConfig = { timeout: 20000 };

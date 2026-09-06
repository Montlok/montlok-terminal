import type { Settings as LayoutSettings } from '@ant-design/pro-components';
import { Link, type RequestConfig, type RunTimeLayoutConfig } from '@umijs/max';
import { ConfigProvider, theme } from 'antd';
import dayjs from 'dayjs';
import relativeTime from 'dayjs/plugin/relativeTime';
import settings from '../config/defaultSettings';
import { ensureSession, type SessionInfo } from './operator/api';
import { HeaderStatus } from './operator/HeaderStatus';
import { QuantWorkspace } from './operator/QuantWorkspace';
import './operator/theme.css';

dayjs.extend(relativeTime);

export async function getInitialState(): Promise<{
  settings?: Partial<LayoutSettings>;
  currentUser?: API.CurrentUser;
  fetchUserInfo?: () => Promise<API.CurrentUser | undefined>;
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
    settings: settings as Partial<LayoutSettings>,
    fetchUserInfo: async () => currentUser,
    settingDrawerOpen: false,
  };
}

export const layout: RunTimeLayoutConfig = () => ({
  ...settings,
  menu: { locale: false },
  menuItemRender: (item, dom) =>
    item.path ? <Link to={item.path}>{dom}</Link> : dom,
  actionsRender: () => [<HeaderStatus key="status" />],
  avatarProps: undefined,
  footerRender: false,
  bgLayoutImgList: [],
  contentStyle: { padding: 0 },
  childrenRender: (children) => (
    <ConfigProvider
      componentSize="small"
      theme={{
        algorithm: theme.darkAlgorithm,
        token: {
          colorPrimary: '#1677ff',
          colorBgBase: '#101214',
          colorBgContainer: '#181b1f',
          colorBorder: '#343a42',
          borderRadius: 4,
          fontSize: 13,
          fontFamily:
            '-apple-system, BlinkMacSystemFont, "PingFang SC", "Segoe UI", sans-serif',
        },
      }}
    >
      <div className="operator-root bp6-dark">
        <QuantWorkspace>{children}</QuantWorkspace>
      </div>
    </ConfigProvider>
  ),
});
export const request: RequestConfig = { timeout: 20000 };

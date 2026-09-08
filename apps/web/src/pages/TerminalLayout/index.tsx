import { MenuFoldOutlined, MenuUnfoldOutlined } from '@ant-design/icons';
import { Link, Outlet, useLocation } from '@umijs/max';
import { Button, ConfigProvider, theme } from 'antd';
import { useState } from 'react';
import { HeaderStatus } from '../../operator/HeaderStatus';
import { navigation } from '../../operator/navigation';
import { QuantWorkspace } from '../../operator/QuantWorkspace';
import './terminalShell.css';

/** The shell owns navigation and geometry only; existing business views keep their APIs. */
export default function TerminalLayout() {
  const { pathname } = useLocation();
  const section =
    navigation.find((item) => pathname.startsWith(item.path + '/')) ||
    navigation[0];
  const [collapsed, setCollapsed] = useState(() => {
    try {
      return localStorage.getItem('montlok.navigation.collapsed') === 'true';
    } catch {
      return false;
    }
  });
  const toggle = () =>
    setCollapsed((value) => {
      try {
        localStorage.setItem('montlok.navigation.collapsed', String(!value));
      } catch {}
      return !value;
    });
  return (
    <ConfigProvider
      componentSize="small"
      theme={{
        algorithm: theme.darkAlgorithm,
        token: {
          colorPrimary: '#2787f5',
          colorBgBase: '#101214',
          colorBgContainer: '#151a20',
          colorBorder: '#2c3640',
          borderRadius: 2,
          fontSize: 12,
          fontFamily:
            '-apple-system, BlinkMacSystemFont, "PingFang SC", "Segoe UI", sans-serif',
        },
      }}
    >
      <div
        className={`terminal-shell ${collapsed ? 'terminal-nav-collapsed' : ''}`}
      >
        <header className="terminal-shell-header">
          <Link className="terminal-brand" to="/workspace">
            MONTLOK
          </Link>
          <nav aria-label="主导航">
            {navigation.map((item) => (
              <Link
                key={item.path}
                to={item.groups[0].pages[0].path}
                aria-current={item.path === section.path ? 'page' : undefined}
              >
                {item.name}
              </Link>
            ))}
          </nav>
          <HeaderStatus />
        </header>
        <aside className="terminal-navigation" aria-label="业务导航">
          <div className="terminal-nav-title">
            <strong>{collapsed ? '' : section.name}</strong>
            <Button
              type="text"
              aria-label={collapsed ? '展开导航' : '收起导航'}
              onClick={toggle}
              icon={collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
            />
          </div>
          {!collapsed &&
            section.groups.map((group) => (
              <details
                key={group.path}
                open={pathname.startsWith(group.path + '/')}
              >
                <summary>{group.name}</summary>
                <nav aria-label={group.name}>
                  {group.pages.map((page) => (
                    <Link
                      key={page.path}
                      to={page.path}
                      aria-current={page.path === pathname ? 'page' : undefined}
                    >
                      {page.name}
                    </Link>
                  ))}
                </nav>
              </details>
            ))}
        </aside>
        <main className="operator-root bp6-dark">
          <QuantWorkspace>
            <Outlet />
          </QuantWorkspace>
        </main>
      </div>
    </ConfigProvider>
  );
}

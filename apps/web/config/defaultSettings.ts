import type { ProLayoutProps } from '@ant-design/pro-components';

const settings: ProLayoutProps = {
  title: 'Montlok',
  logo: false,
  navTheme: 'realDark',
  colorPrimary: '#1677ff',
  layout: 'mix',
  splitMenus: true,
  contentWidth: 'Fluid',
  fixedHeader: true,
  fixSiderbar: true,
  siderWidth: 192,
  token: {
    bgLayout: '#101214',
    header: {
      colorBgHeader: '#121416',
      colorHeaderTitle: '#e6e8eb',
      heightLayoutHeader: 48,
      colorBgMenuItemSelected: '#1b2430',
      colorTextMenuSelected: '#e6e8eb',
    },
    sider: {
      colorMenuBackground: '#121416',
      colorTextMenu: '#939ba5',
      colorTextMenuSelected: '#dfe8ff',
      colorBgMenuItemSelected: '#1b2430',
      menuHeight: 36,
    },
    pageContainer: {
      paddingInlinePageContainerContent: 0,
      paddingBlockPageContainerContent: 0,
    },
  },
};
export default settings;

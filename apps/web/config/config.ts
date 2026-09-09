import { defineConfig } from '@umijs/max';
import routes from './routes';

const terminalBase = process.env.MONTLOK_WEB_BASE || '/';
if (!/^\/(?:[a-zA-Z0-9_-]+\/)*$/.test(terminalBase))
  throw new Error('Invalid terminal base path');

export default defineConfig({
  hash: true,
  base: terminalBase,
  publicPath: terminalBase,
  title: 'Montlok',
  routes,
  model: {},
  initialState: {},
  access: {},
  request: {},
  reactQuery: {},
  layout: false,
  locale: { default: 'zh-CN', antd: true, baseNavigator: false },
  antd: { appConfig: {} },
  proxy: {
    '/api/': {
      target: 'http://127.0.0.1:18081',
      changeOrigin: false,
      ws: true,
    },
  },
  fastRefresh: true,
  mock: false,
  utoopack: {},
  define: {
    'process.env.MONTLOK_WEB_BASE': terminalBase,
    __APP_VERSION__: '0.2.0',
    __UMI_VERSION__: '4',
    __UTOO_VERSION__: 'operator',
    'process.env.COMMIT_HASH': 'AntPro-adfd440',
  },
});

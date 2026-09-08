import { defineConfig } from '@umijs/max';
import routes from './routes';

export default defineConfig({
  hash: true,
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
    __APP_VERSION__: '0.2.0',
    __UMI_VERSION__: '4',
    __UTOO_VERSION__: 'operator',
    'process.env.COMMIT_HASH': 'AntPro-adfd440',
  },
});

import { navigation } from '../src/operator/navigation';

export default [
  { path: '/login', component: './OperatorLogin', layout: false },
  { path: '/device', component: './DeviceAuthorization', layout: false },
  {
    path: '/',
    component: './TerminalLayout',
    routes: [
      { path: '/', redirect: '/workspace/portfolio/overview' },
      ...navigation.map((section) => ({
        path: section.path,
        name: section.name,
        icon: section.icon,
        routes: [
          { path: section.path, redirect: section.groups[0].pages[0].path },
          ...section.groups.map((group) => ({
            path: group.path,
            name: group.name,
            routes: [
              { path: group.path, redirect: group.pages[0].path },
              ...group.pages.map((page) => ({
                path: page.path,
                name: page.name,
                component: page.component || './ApiWorkbench',
              })),
            ],
          })),
        ],
      })),
      { path: '/account', redirect: '/assets/account/overview' },
      { path: '/api', redirect: '/settings/developer/api' },
      { path: '/connections', redirect: '/settings/accounts/connections' },
      { path: '/operations', redirect: '/settings/system/operations' },
      { path: '*', component: './exception/404' },
    ],
  },
];

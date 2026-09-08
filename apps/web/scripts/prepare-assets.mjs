import { mkdir,copyFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { dirname,resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
const here=dirname(fileURLToPath(import.meta.url));const root=resolve(here,'..');
const require=createRequire(import.meta.url);const output=resolve(root,'public/vendor/perspective-5.3.1');
await mkdir(output,{recursive:true});
for(const [module,file] of [
  ['@perspective-dev/client','perspective-js.wasm'],
  ['@perspective-dev/server','perspective-server.wasm'],
  ['@perspective-dev/viewer','perspective-viewer.wasm'],
]){
  const packageRoot=dirname(require.resolve(`${module}/package.json`));
  await copyFile(resolve(packageRoot,'dist/wasm',file),resolve(output,file));
}
for(const [module,file] of [
  ['@perspective-dev/client','perspective.js'],
  ['@perspective-dev/client','perspective-server.worker.js'],
  ['@perspective-dev/viewer','perspective-viewer.js'],
  ['@perspective-dev/viewer-datagrid','perspective-viewer-datagrid.js'],
]){
  const packageRoot=dirname(require.resolve(`${module}/package.json`));
  await copyFile(resolve(packageRoot,'dist/cdn',file),resolve(output,file));
  await copyFile(resolve(packageRoot,'dist/cdn',file+'.map'),resolve(output,file+'.map'));
}

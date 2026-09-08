import { mkdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { resolve, dirname } from 'node:path';
import { spawnSync } from 'node:child_process';
const here=dirname(fileURLToPath(import.meta.url));
const root=resolve(here,'../..');
const output=resolve(here,'src/generated');
mkdirSync(output,{recursive:true});
const plugin=resolve(root,'node_modules/.bin/protoc-gen-ts_proto');
const args=[`--plugin=protoc-gen-ts_proto=${plugin}`,`--ts_proto_out=${output}`,
  '--ts_proto_opt=forceLong=bigint,esModuleInterop=true,oneof=unions,outputServices=false,useOptionals=messages',
  `--proto_path=${resolve(here,'../contracts/proto')}`,
  resolve(here,'../contracts/proto/montlok/v2/terminal.proto')];
let result=spawnSync('protoc',args,{stdio:'inherit'});if(result.status!==0)process.exit(result.status??1);
result=spawnSync(resolve(root,'node_modules/.bin/openapi-typescript'),[resolve(here,'../contracts/openapi/montlok-v2.yaml'),'-o',resolve(output,'api.ts')],{stdio:'inherit'});
process.exit(result.status??1);

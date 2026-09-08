import fs from 'node:fs';
import {fileURLToPath} from 'node:url';
import {dirname,resolve} from 'node:path';
import YAML from 'yaml';
import Ajv from 'ajv/dist/2020.js';
const here=dirname(fileURLToPath(import.meta.url));const root=resolve(here,'../..');
const read=(path)=>fs.readFileSync(resolve(root,path),'utf8');
const fields=[...YAML.parse(read('packages/contracts/fields/core.yaml')).fields,...JSON.parse(read('packages/contracts/fields/terminal.json')).fields];
const capabilities=YAML.parse(read('capabilities.yaml')).capabilities;
const schema=JSON.parse(read('packages/contracts/schemas/field-definition.schema.json'));
const workspaceSchema=JSON.parse(read('packages/contracts/schemas/workspace.schema.json'));
const ajv=new Ajv({allErrors:true,strict:true});const validateField=ajv.compile(schema);const validateWorkspace=ajv.compile(workspaceSchema);
const ids=new Set();
for(const field of fields){if(!validateField(field))throw new Error(`${field.id}: ${JSON.stringify(validateField.errors)}`);if(ids.has(field.id))throw new Error(`Duplicate field ${field.id}`);ids.add(field.id);}
const capIds=new Set(capabilities.map(capability=>capability.id));if(capIds.size!==capabilities.length)throw new Error('Duplicate capability');
for(const file of fs.readdirSync(resolve(root,'packages/workspaces')).filter(file=>file.endsWith('.json'))){
  const workspace=JSON.parse(read('packages/workspaces/'+file));if(!validateWorkspace(workspace))throw new Error(`${file}: ${JSON.stringify(validateWorkspace.errors)}`);
  for(const panel of workspace.panels){if(!capIds.has(panel.capability))throw new Error(`${panel.id} references missing capability`);for(const field of panel.fields)if(!ids.has(field))throw new Error(`${panel.id} references missing field ${field}`);}
}
console.log(`${fields.length} fields, ${capabilities.length} capabilities, 6 workspaces validated`);

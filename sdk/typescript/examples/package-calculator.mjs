// Package the Calculator example for Loader update examples and tests.
// Bundle the installed, lockfile-selected import closure. Deployments do not resolve packages.
import { build } from 'esbuild';
import { writeFile, mkdir } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
const [output,version='v1']=process.argv.slice(2);
if(!output || !['v1','v2'].includes(version))throw new Error('usage: node sdk/typescript/examples/package-calculator.mjs <output.json> [v1|v2]');
const bundle=await build({absWorkingDir:dirname(fileURLToPath(import.meta.url)),entryPoints:['calculator.ts'],bundle:true,write:false,platform:'node',target:'node24',format:'esm',legalComments:'inline',
  plugins:[{name:'calculator-version',setup(build){build.onLoad({filter:/calculator-version\.ts$/},()=>({contents:`export const codeVersion=${JSON.stringify(version)};`,loader:'ts'}));}}]});
const target = `${process.arch === 'arm64' ? 'aarch64' : 'x86_64'}-${{darwin:'apple-darwin',linux:'unknown-linux-gnu',win32:'pc-windows-msvc'}[process.platform]}`;
const content = bundle.outputFiles[0].contents;
const artifact={target,entry:'calculator.mjs',files:[{path:'calculator.mjs',kind:'data',sha256:createHash('sha256').update(content).digest('hex')}],host_dependencies:{interpreter:`node:${process.version}`}};
await mkdir(dirname(resolve(output)),{recursive:true});
await writeFile(resolve(dirname(output),'calculator.mjs'),content);
await writeFile(output,JSON.stringify(artifact,null,2));
console.log(`Packaged calculator ${version}: ${output}`);

import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

// npm supplies this workspace's executable search path. Keep repository-relative
// input/output paths stable when npm switches into tools/codegen/typescript to run us.
const root = fileURLToPath(new URL('../../../', import.meta.url));
const python = process.env.PYTHON ?? 'python3';
const [command, ...args] = process.argv.slice(2);

function run(program, arguments_) {
  const result = spawnSync(program, arguments_, { cwd: root, stdio: 'inherit' });
  if (result.error) {
    console.error(`Cannot run ${program}: ${result.error.message}`);
    process.exit(1);
  }
  if (result.status !== 0) {
    if (result.signal) console.error(`${program} terminated by ${result.signal}`);
    process.exit(result.status ?? 1);
  }
}

switch (command) {
  case 'codegen':
    run('cargo', ['run', '--quiet', '--locked', '-p', 'cordis-codegen', '--', ...args]);
    break;
  case 'generate':
    run(python, ['tools/codegen/build.py']);
    run('cargo', ['build', '--locked', '-p', 'cordis-codegen']);
    for (const owner of ['examples', 'tests']) {
      const compiler = process.platform === 'win32' ? 'cordis-codegen.exe' : 'cordis-codegen';
      const target = process.env.CARGO_TARGET_DIR ?? 'target';
      const options = ['--codegen', `${target}/debug/${compiler}`];
      run(python, [`${owner}/build.py`, ...options, ...args]);
    }
    break;
  default:
    console.error('Expected codegen or generate');
    process.exit(1);
}

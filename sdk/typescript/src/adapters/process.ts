import type { Service } from '../core/service.ts';
import { serve } from './session/host.ts';
import { listen } from '../protocols/http/server.ts';
import { readFrames, writeFrame } from '../transports/stdio.ts';

/** Own the Node process carrier. Business lifecycle remains in the common
 * Session implementation; this entry alone knows stdin, stdout and HTTP. */
export async function run(service: Service, options: { http?: boolean } = {}): Promise<void> {
  try {
    await serve(service, {
      receive: (validation, capacity) => readFrames(process.stdin, validation, capacity),
      write: bytes => writeFrame(process.stdout, bytes),
      close: () => process.stdin.destroy(),
    }, (state, dispatch) => listen(options.http !== false, state, dispatch));
  } catch (error) {
    process.exitCode = 1;
    throw error;
  }
}

import { body } from '../binary-support.ts';
import { listen } from '../../src/protocols/http/server.ts';
import { encodeFrame } from '../../src/protocols/session/binary.ts';
import { payload, type Request, type Response } from '../../src/protocols/session/messages.ts';
import type { Value } from '../../src/core/value.ts';
import { readFrames, writeFrame } from '../../src/transports/stdio.ts';

let release: (() => void) | undefined;
let output = Promise.resolve();
let capacity = 1024 * 1024;
let ioTimeout = 5000;
const http = await listen(true, { active: () => true, validation: () => 'checked', capacity: () => capacity, ioTimeout: () => ioTimeout, connections: () => 64 }, dispatch);

async function dispatch(request: Request): Promise<Response> {
  let result = payload();
  const args = request.payload.control as Record<string, Value>;
  if (request.command === 'hello') {
    capacity = Number(args.frame_capacity);
    ioTimeout = Number(args.io_timeout_ms);
    result = payload({...args, http: http.url, capabilities: { multiplex: true, max_inflight: 16 }});
  }
  else if (request.command === 'call') {
    const method = args.method;
    if (method === 'hold') await new Promise<void>(resolve => { release = resolve; });
    if (method === 'release') { release?.(); release = undefined; }
    result = method === 'status' ? payload(null, body(release !== undefined)) : payload(null, request.payload.messages[0]);
  }
  return {
    session: request.session, activation: request.activation, operation: request.operation,
    outcome: { kind: 'success', value: result },
  };
}

for await (const request of readFrames(process.stdin, undefined, () => capacity)) {
  if (!('command' in request)) throw new Error('expected request');
  void dispatch(request).then(async response => {
    output = output.then(() => writeFrame(process.stdout, encodeFrame(response, capacity)));
    await output;
    if (request.command === 'deactivate') {
      await http.close();
      process.exit(0);
    }
  });
}

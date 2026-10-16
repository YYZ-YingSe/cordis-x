import { createServer } from 'node:http';
import { once } from 'node:events';
import { MAX_MESSAGE } from '../../core/value.ts';
import { CONTENT_TYPE, decodeFrame, encodeFrame } from '../session/binary.ts';
import type { Request, Response } from '../session/messages.ts';
import type { Validation } from '../../core/validation.ts';
export async function listen(enabled: boolean, state: { active(): boolean; validation(): Validation }, dispatch: (request: Request) => Promise<Response>) {
  let url: string | null = null;
  const server = createServer({ maxHeaderSize: 16384, requestTimeout: 5000 }, async (request, response) => {
    try {
      if (!state.active() || request.method !== 'POST' || request.url !== '/cordis/call' ||
          request.headers['content-type'] !== CONTENT_TYPE || request.headers['transfer-encoding'])
        throw new Error('unsupported HTTP request');
      const length = request.headers['content-length'];
      if (!length || !/^[0-9]{1,7}$/.test(length) || Number(length) > MAX_MESSAGE) throw new Error('invalid Content-Length');
      const chunks: Buffer[] = []; let size = 0;
      for await (const chunk of request) {
        size += chunk.length;
        if (size > MAX_MESSAGE) throw new Error('message too large');
        chunks.push(chunk);
      }
      if (size !== Number(length)) throw new Error('truncated body');
      const frame = decodeFrame(chunks.length === 1 ? chunks[0] : Buffer.concat(chunks, size), state.validation());
      if (!('command' in frame) || !['call', 'resource_acquire', 'resource_call', 'resource_release'].includes(frame.command))
        throw new Error('command requires session endpoint');
      const result = await dispatch(frame);
      const body = encodeFrame(result);
      response.writeHead(result.outcome.kind === 'success' ? 200 : 400, {
        'content-type': CONTENT_TYPE, 'content-length': body.byteLength, 
      });
      response.end(body);
    } catch { request.destroy(); response.destroy(); }
  });
  server.maxConnections = 32;
  if (enabled) {
    server.listen(0, '127.0.0.1'); await once(server, 'listening');
    const address = server.address();
    if (!address || typeof address === 'string') throw new Error('missing listen address');
    url = `http://127.0.0.1:${address.port}/cordis/call`;
  }
  return { url, async close() { if (server.listening) { server.close(); server.closeAllConnections(); await once(server, 'close'); } } };
}

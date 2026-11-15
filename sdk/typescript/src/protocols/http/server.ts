import { createServer } from 'node:http';
import { once } from 'node:events';
import type { Socket } from 'node:net';
import { CONTENT_TYPE, decodeFrame, encodeFrame } from '../session/binary.ts';
import type { Request, Response } from '../session/messages.ts';
import type { Validation } from '../../core/validation.ts';
export async function listen(enabled: boolean, state: { active(): boolean; validation(): Validation; capacity(): number; ioTimeout(): number; connections(): number }, dispatch: (request: Request) => Promise<Response>) {
  let url: string | null = null;
  let inflight = 0;
  const connections = new Map<Socket, { pending: number; deadline?: NodeJS.Timeout }>();
  const idle = (socket: Socket) => {
    const connection = connections.get(socket);
    if (!connection || connection.pending || socket.destroyed) return;
    clearTimeout(connection.deadline);
    connection.deadline = setTimeout(() => socket.destroy(), state.ioTimeout());
  };
  const server = createServer({ maxHeaderSize: 16384, requestTimeout: 0, headersTimeout: 0, keepAliveTimeout: 0 }, async (request, response) => {
    const socket = request.socket, connection = connections.get(socket);
    if (!connection || inflight >= state.connections()) { request.destroy(); response.destroy(); return; }
    clearTimeout(connection.deadline);
    connection.pending++;
    inflight++;
    let deadline: NodeJS.Timeout | undefined = setTimeout(() => request.destroy(), state.ioTimeout());
    let released = false, settled = false, closed = false;
    const release = () => { if (!released && settled && closed) { released = true; inflight--; } };
    response.once('close', () => {
      clearTimeout(deadline);
      connection.pending--;
      idle(socket);
      closed = true;
      release();
    });
    try {
      if (!state.active() || request.method !== 'POST' || request.url !== '/cordis/call' ||
          request.headers['content-type'] !== CONTENT_TYPE || request.headers['transfer-encoding'])
        throw new Error('unsupported HTTP request');
      const length = request.headers['content-length'];
      if (!length || !/^[0-9]{1,10}$/.test(length) || Number(length) > state.capacity()) throw new Error('invalid Content-Length');
      const chunks: Buffer[] = []; let size = 0;
      for await (const chunk of request) {
        size += chunk.length;
        if (size > state.capacity()) throw new Error('message too large');
        chunks.push(chunk);
      }
      if (size !== Number(length)) throw new Error('truncated body');
      const frame = decodeFrame(chunks.length === 1 ? chunks[0] : Buffer.concat(chunks, size), state.validation());
      if (!('command' in frame) || !['call', 'resource_acquire', 'resource_call', 'resource_release'].includes(frame.command))
        throw new Error('command requires session endpoint');
      clearTimeout(deadline);
      const result = await dispatch(frame);
      if (response.destroyed) return;
      const body = encodeFrame(result, state.capacity());
      deadline = setTimeout(() => response.destroy(), state.ioTimeout());
      response.writeHead(result.outcome.kind === 'success' ? 200 : 400, {
        'content-type': CONTENT_TYPE, 'content-length': body.byteLength, 
      });
      response.end(body);
    } catch { clearTimeout(deadline); request.destroy(); response.destroy(); }
    finally { settled = true; release(); }
  });
  server.on('connection', socket => {
    if (!state.active() || connections.size >= state.connections()) { socket.destroy(); return; }
    connections.set(socket, { pending: 0 });
    socket.once('close', () => {
      clearTimeout(connections.get(socket)?.deadline);
      connections.delete(socket);
    });
    idle(socket);
  });
  if (enabled) {
    server.listen(0, '127.0.0.1'); await once(server, 'listening');
    const address = server.address();
    if (!address || typeof address === 'string') throw new Error('missing listen address');
    url = `http://127.0.0.1:${address.port}/cordis/call`;
  }
  return { url, async close() { if (server.listening) { server.close(); server.closeAllConnections(); await once(server, 'close'); } } };
}

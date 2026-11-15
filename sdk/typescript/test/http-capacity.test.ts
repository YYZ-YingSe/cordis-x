import assert from 'node:assert/strict';
import test from 'node:test';
import { request as httpRequest } from 'node:http';
import { setImmediate } from 'node:timers/promises';
import { setTimeout as delay } from 'node:timers/promises';
import { connect } from 'node:net';
import { listen } from '../src/protocols/http/server.ts';
import { CONTENT_TYPE, encodeFrame } from '../src/protocols/session/binary.ts';
import { payload, type Request } from '../src/protocols/session/messages.ts';

test('host I/O deadlines bound incomplete input, not admitted business execution', { timeout: 10_000 }, async () => {
  const entered = Promise.withResolvers<void>(), release = Promise.withResolvers<void>();
  const server = await listen(true, { active: () => true, validation: () => 'checked', capacity: () => 1024 * 1024, ioTimeout: () => 100, connections: () => 8 }, async request => {
    entered.resolve();
    await release.promise;
    return { session: request.session, activation: request.activation, operation: request.operation, outcome: { kind: 'success', value: payload(null) } };
  });
  const url = new URL(server.url!);
  const body = encodeFrame({ session: 'a'.repeat(32), activation: { incarnation: '1', fiber: '2', epoch: '3' }, operation: '0:1', command: 'call', payload: payload(null) });
  try {
    for (const partial of ['POST /cordis/call HTTP/1.1\r\n', `POST /cordis/call HTTP/1.1\r\nHost: localhost\r\nContent-Type: ${CONTENT_TYPE}\r\nContent-Length: ${body.length}\r\n\r\n`]) {
      const socket = connect(Number(url.port), url.hostname);
      socket.on('error', () => {});
      const closed = new Promise<void>(resolve => socket.once('close', () => resolve()));
      socket.write(partial);
      await closed;
    }
    const request = httpRequest(url, { method: 'POST', agent: false, headers: { 'content-type': CONTENT_TYPE, 'content-length': body.length } });
    const finished = new Promise<number | undefined>((resolve, reject) => {
      request.on('error', reject);
      request.on('response', response => { response.resume(); response.once('end', () => resolve(response.statusCode)); });
    });
    request.end(body);
    await entered.promise;
    await delay(300);
    assert(!request.destroyed);
    release.resolve();
    assert.equal(await finished, 200);
  } finally {
    release.resolve();
    await server.close();
  }
});

test('disconnected HTTP waiters retain body admission until dispatch settles', {timeout: 20_000}, async () => {
  const settlement = Promise.withResolvers<void>();
  let admission = Promise.withResolvers<void>();
  let dispatched = 0;
  const capacity = 6;
  const server = await listen(true, {active: () => true, validation: () => 'checked', capacity: () => 1024 * 1024, ioTimeout: () => 100, connections: () => capacity}, async request => {
    dispatched++;
    admission.resolve();
    await settlement.promise;
    return {session: request.session, activation: request.activation, operation: request.operation, outcome: {kind: 'success', value: payload(null)}};
  });
  const body = encodeFrame({
    session: 'a'.repeat(32), activation: {incarnation: '1', fiber: '2', epoch: '3'},
    operation: '1:1', command: 'call', payload: payload({method: 'echo'}),
  } satisfies Request);
  const send = () => {
    const request = httpRequest(server.url!, {method: 'POST', agent: false, headers: {'content-type': CONTENT_TYPE, 'content-length': body.byteLength}});
    const finished = new Promise<boolean>(resolve => {
      request.on('error', () => resolve(false));
      request.on('response', response => { response.resume(); response.on('end', () => resolve(response.statusCode === 200)); });
    });
    request.end(body);
    return {request, finished};
  };
  try {
    for (let index = 0; index < capacity; index++) {
      admission = Promise.withResolvers<void>();
      const call = send();
      await admission.promise;
      call.request.destroy();
      assert.equal(await call.finished, false);
    }
    admission = Promise.withResolvers<void>();
    const refused = send();
    assert.equal(await Promise.race([refused.finished, admission.promise.then(() => 'admitted')]), false);
    assert.equal(dispatched, capacity);
    settlement.resolve();
    await setImmediate();
    await setImmediate();
    assert.equal(await send().finished, true);
  } finally {
    settlement.resolve();
    await server.close();
  }
});

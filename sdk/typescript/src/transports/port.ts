import { decodeFrame } from '../protocols/session/binary.ts';
import type { Connection } from './connection.ts';

/** Structural MessagePort face, also implemented by test and embedding hosts.
 * The carrier transmits a complete Cordis binary frame per message. It never
 * serializes business objects or creates a second request identity namespace. */
export interface BinaryPort {
  postMessage(message: Uint8Array): void;
  addEventListener(type: 'message' | 'messageerror', listener: (event: { data: unknown }) => void): void;
  removeEventListener(type: 'message' | 'messageerror', listener: (event: { data: unknown }) => void): void;
  start(): void;
  close(): void;
}

/** Turn an owned port into a Session carrier. A bounded incoming queue protects
 * a stalled consumer; exhaustion closes the carrier and preserves lost-result
 * responsibility instead of pretending an already sent operation was refused. */
export function portConnection(port: BinaryPort, bufferedBytes: number): Connection {
  if (!Number.isSafeInteger(bufferedBytes) || bufferedBytes <= 0)
    throw new Error('port buffer capacity must be a positive safe integer');
  const queue: Uint8Array[] = [];
  let bytes = 0, closed = false, failure: Error | undefined;
  let wake: (() => void) | undefined;
  const close = () => {
    if (closed) return;
    closed = true;
    port.removeEventListener('message', receive);
    port.removeEventListener('messageerror', invalid);
    port.close();
    wake?.();
  };
  const invalid = () => { failure = new Error('invalid Session carrier message'); close(); };
  const receive = (event: { data: unknown }) => {
    if (!(event.data instanceof Uint8Array) || bytes + event.data.byteLength > bufferedBytes) {
      invalid(); return;
    }
    queue.push(event.data);
    bytes += event.data.byteLength;
    wake?.();
  };
  port.addEventListener('message', receive);
  port.addEventListener('messageerror', invalid);
  port.start();
  return {
    async *receive(validation, capacity) {
      while (!closed || queue.length) {
        const value = queue.shift();
        if (!value) { await new Promise<void>(resolve => { wake = resolve; }); wake = undefined; continue; }
        bytes -= value.byteLength;
        if (!value.byteLength || value.byteLength > capacity()) throw new Error('invalid frame length');
        yield decodeFrame(value, validation());
      }
      if (failure) throw failure;
    },
    async write(value) {
      if (closed) throw new Error('Session carrier is closed');
      port.postMessage(value);
    },
    close,
  };
}

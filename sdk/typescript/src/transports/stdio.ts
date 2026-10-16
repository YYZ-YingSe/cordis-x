import type { Readable, Writable } from 'node:stream';
import { decodeFrame } from '../protocols/session/binary.ts';
import type { Request, Response } from '../protocols/session/messages.ts';
import { MAX_MESSAGE } from '../core/value.ts';
import type { Validation } from '../core/validation.ts';

/** The producer owns encoding and admission. This writer never re-encodes. */
export async function writeFrame(output: Writable, body: Uint8Array): Promise<void> {
  if (!body.byteLength || body.byteLength > MAX_MESSAGE) throw new Error('invalid frame length');
  const frame = Buffer.allocUnsafe(4 + body.byteLength);
  frame.writeUInt32BE(body.byteLength);
  frame.set(body, 4);
  await new Promise<void>((resolve, reject) => output.write(frame, error => error ? reject(error) : resolve()));
}
export async function* readFrames(input: Readable, validation: () => Validation = () => 'checked'): AsyncGenerator<Request | Response> {
  const header = Buffer.allocUnsafe(4);
  let headerBytes = 0, body: Buffer | undefined, bodyBytes = 0;
  for await (const chunk of input) {
    if (!(chunk instanceof Uint8Array)) throw new Error('expected binary stream');
    let offset = 0;
    while (offset < chunk.byteLength) {
      if (!body) {
        const n = Math.min(4 - headerBytes, chunk.byteLength - offset);
        header.set(chunk.subarray(offset, offset + n), headerBytes);
        headerBytes += n; offset += n;
        if (headerBytes !== 4) continue;
        const length = header.readUInt32BE();
        if (length < 1 || length > MAX_MESSAGE) throw new Error('invalid frame length');
        // One owned allocation per frame. Never reuse it after yielding: the
        // decoder may retain views, and the producer may mutate its own chunks.
        body = Buffer.allocUnsafe(length); bodyBytes = 0;
      }
      const n = Math.min(body.byteLength - bodyBytes, chunk.byteLength - offset);
      body.set(chunk.subarray(offset, offset + n), bodyBytes);
      bodyBytes += n; offset += n;
      if (bodyBytes === body.byteLength) {
        const frame = decodeFrame(body, validation());
        body = undefined; bodyBytes = 0; headerBytes = 0;
        yield frame;
      }
    }
  }
  if (headerBytes || body) throw new Error('truncated frame');
}

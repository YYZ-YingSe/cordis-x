/** Encoding primitives shared by browser and server adapters. Native platform
 * objects must not leak into generated contracts or the common Session codec. */
const encoder = new TextEncoder();
export function utf8Length(value: string): number {
  return encoder.encode(value).byteLength;
}

/** Decode the descriptor representation. The caller checks canonical encoding
 * and its existing descriptor budget before constructing a schema registry. */
export function decodeBase64(value: string): Uint8Array {
  return Uint8Array.from(atob(value), character => character.charCodeAt(0));
}

/** Encode in bounded pieces without spreading an unbounded byte array onto the
 * JavaScript call stack. This representation is identical on every adapter. */
export function encodeBase64(value: Uint8Array): string {
  const chunks: string[] = [];
  for (let offset = 0; offset < value.length; offset += 8192)
    chunks.push(String.fromCharCode(...value.subarray(offset, offset + 8192)));
  return btoa(chunks.join(''));
}

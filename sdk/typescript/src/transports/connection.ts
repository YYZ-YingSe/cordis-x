import type { Request, Response, CancelObservation } from '../protocols/session/messages.ts';
import type { Validation } from '../core/validation.ts';

/** A connected carrier for the common Cordis Session protocol. Closing a
 * carrier stops reception; accepted calls still drain in the Session owner.
 * Implementations enforce the negotiated frame capacity before decoding. */
export interface Connection {
  receive(validation: () => Validation, capacity: () => number): AsyncIterable<Request | Response | CancelObservation>;
  write(bytes: Uint8Array): Promise<void>;
  close(): void;
}

/** Optional ordinary-call endpoint owned by a platform adapter. Session control
 * traffic always stays on the connected carrier. Browsers need no HTTP server. */
export interface CallEndpoint {
  url: string | null;
  close(): Promise<void>;
}

export type EndpointFactory = (
  state: { active(): boolean; validation(): Validation; capacity(): number; ioTimeout(): number; connections(): number },
  dispatch: (request: Request) => Promise<Response>,
) => Promise<CallEndpoint>;

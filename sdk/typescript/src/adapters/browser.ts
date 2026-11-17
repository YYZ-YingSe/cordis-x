/** Browser services participate in the same Session protocol as server-side
 * services. The embedding application supplies a connected binary carrier;
 * there is no subprocess, Node polyfill or browser-specific business RPC. */
export { serve } from './session/host.ts';
export { portConnection } from '../transports/port.ts';
export type { BinaryPort } from '../transports/port.ts';
export type { Connection } from '../transports/connection.ts';

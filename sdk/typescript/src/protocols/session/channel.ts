import type { Message } from '../../core/message.ts';
import { encodeFrame } from './binary.ts';
import { payload, one, controlOnly, type Payload, type Request, type Response } from './messages.ts';
import { Refusal } from '../../core/error.ts';
import { RpcError } from '../../core/error.ts';
import type { Value } from '../../core/value.ts';
import type { Identity } from './messages.ts';
export type { Identity, Envelope } from './messages.ts';
type Pending = { lane: number; command: string; resolve(value: Payload): void; reject(error: Error): void };

export class DependencyChannel {
  validation: import('../../core/validation.ts').Validation = 'checked';
  private pending = new Map<string, Pending>();
  private next = 0;
  private session = '';
  private cleanupCalls = false;
  private metadataEnabled = false;
  private resourcesEnabled = false;
  private nextResource = 0;
  enableResources(enabled: boolean) {
    this.resourcesEnabled = enabled;
  }
  resourceId(): string {
    if (!this.resourcesEnabled)
      throw new Refusal('unsupported_resources', 'dependency resources unavailable');
    if (this.nextResource >= 4096)
      throw new Refusal('resources_exhausted', 'resource history full');
    return `r${++this.nextResource}`;
  }
  enableMetadata(enabled: boolean) {
    this.metadataEnabled = enabled;
  }
  metadata(context: string, name: string, revision: string, cleanup: boolean): Promise<Value> {
    if (!this.metadataEnabled)
      return Promise.reject(new Refusal('unsupported_metadata', 'metadata not negotiated'));
    return this.request(cleanup ? 'cleanup_metadata' : 'context_metadata', {
      context,
      name,
      revision,
    });
  }
  reserveCleanup(enabled: boolean) {
    this.cleanupCalls = enabled;
  }
  private activation: Identity | undefined;
  private output: ((bytes: Uint8Array) => Promise<void>) | undefined;
  configure(session: string, activation: Identity, output: (bytes: Uint8Array) => Promise<void>) {
    this.session = session;
    this.activation = activation;
    this.output = output;
  }
  async invoke(context: string, dependency: string, method: string, args: Message, cleanup = false): Promise<Message> {
    return one(await this.requestPayload(cleanup && this.cleanupCalls ? 'cleanup_call' : 'dependency_call', payload({ context, dependency, method }, args)));
  }
  async request(command: string, control: Value): Promise<Value> {
    return controlOnly(await this.requestPayload(command, payload(control)));
  }
  requestPayload(command: string, payload: Payload): Promise<Payload> {
    if (!this.output) return Promise.reject(new Refusal('closed', 'dependency channel closed'));
    const lane =
      command === 'dependency_resource_call'
        ? 13
        : command === 'child_install' || command === 'value_provide'
          ? 12
          : command === 'child_ready' || command === 'child_settled'
            ? 10
            : command === 'child_dispose'
              ? 11
              : command === 'middleware_run'
                ? 8
                : command === 'middleware_next'
                  ? 9
                  : command === 'event_emit'
                    ? 6
                    : command === 'event_join'
                      ? 7
                      : ['cleanup_call', 'cleanup_metadata', 'cleanup_resource_call'].includes(command)
                        ? 5
                        : command === 'sequence_ready' || command === 'scope_ready'
                          ? 2
                          : [
                                'effect_dispose',
                                'sequence_dispose',
                                'scope_dispose',
                                'event_dispose',
                                'middleware_dispose',
                                'dependency_resource_dispose',
                              ].includes(command)
                            ? 1
                            : command === 'scope_run'
                              ? 3
                              : command === 'task_join'
                                ? 4
                                : 0;
    if ([...this.pending.values()].filter((entry) => entry.lane === lane).length >= 16)
      return Promise.reject(new Refusal('overloaded', 'dependency call capacity reached'));
    if (this.next >= 65536)
      return Promise.reject(new Refusal('session_exhausted', 'dependency operation history full'));
    const operation = String(++this.next);
    let bytes: Uint8Array;
    try { bytes = encodeFrame({
        session: this.session,
        activation: this.activation!,
        operation,
        command,
        payload,
      }); } catch (error) { return Promise.reject(new Refusal('invalid_argument', String(error))); }
    return new Promise((resolve, reject) => {
      this.pending.set(operation, { lane, command, resolve, reject });
      this.output!(bytes).catch((error) => {
        this.pending.delete(operation);
        reject(error);
      });
    });
  }
  receive(response: Response) {
    const activation = response.activation;
    if (response.session !== this.session || !this.activation ||
        activation.incarnation !== this.activation.incarnation || activation.fiber !== this.activation.fiber || activation.epoch !== this.activation.epoch)
      throw new Error('dependency response identity mismatch');
    const entry = this.pending.get(response.operation);
    if (!entry) throw new Error('unsolicited dependency response');
    const outcome = response.outcome;
    this.pending.delete(response.operation);
    if (outcome.kind === 'success') entry.resolve(outcome.value);
    else {
      const error = outcome.error;
      entry.reject(outcome.kind === 'refused' ? new Refusal(error.code, error.message) : outcome.kind === 'unknown'
        ? new Error(`outcome unknown: ${error.code}: ${error.message}`)
        : new RpcError(error.code, error.message));
    }
  }
  close() {
    this.output = undefined;
    for (const entry of this.pending.values())
      entry.reject(Object.assign(new Error('dependency result lost with channel'), { code: 'closed' }));
    this.pending.clear();
  }
}

export class RpcError extends Error {
  code: string;
  constructor(code: string, message: string) {
    super(message);
    this.code = code;
  }
}

/** Runtime admission or authority refusal; never a business rejection. */
export class Refusal extends RpcError {}

/** Explicit JSON fixture ingress; production adapters accept protobuf messages. */
import { fromJson, toJson, fromBinary, toBinary } from '@bufbuild/protobuf';
import { ValueSchema } from '@bufbuild/protobuf/wkt';
import { decodeFrame } from '../src/protocols/session/binary.ts';
import { payload, type Request, type Payload, type Outcome, type Response } from '../src/protocols/session/messages.ts';
import type { Value } from '../src/core/value.ts';
export { payload };
export type { Request, Payload };
export const body=(value:Value):Uint8Array=>toBinary(ValueSchema,fromJson(ValueSchema,value));
export const jsonBody=(bytes:Uint8Array):Value=>toJson(ValueSchema,fromBinary(ValueSchema,bytes)) as Value;
export function request(bytes:Uint8Array):Request {
  const frame=decodeFrame(bytes);
  if(!('command' in frame))throw new Error('request expected');
  return frame;
}
export function answer(req:Request,value:Value):Payload {
  if(req.payload.messages.length && ['dependency_call','cleanup_call','middleware_run','middleware_next'].includes(req.command))
    return payload(null,body(value));
  if(req.payload.messages.length && ['dependency_resource_call','cleanup_resource_call'].includes(req.command)) {
    const result=value as Record<string,Value>;
    if(result.status==='ok')return payload({status:'ok',value:null},body(result.value));
  }
  return payload(value);
}
export type FixtureOutcome={kind:'success';value:Value}|Exclude<Outcome,{kind:'success'}>;
export function response(req:Request,outcome:FixtureOutcome):Response {
  return {session:req.session,activation:req.activation,operation:req.operation,outcome:outcome.kind==='success'?{kind:'success',value:answer(req,outcome.value)}:outcome};
}

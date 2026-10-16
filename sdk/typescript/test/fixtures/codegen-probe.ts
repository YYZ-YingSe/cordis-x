import {messageType} from '../../src/core/protobuf.ts';
import {operation} from '../../src/core/contract.ts';
import { create,fromJson,toJson,fromBinary,toBinary } from '@bufbuild/protobuf';
import { Int64Schema } from '@cordis-x/test-contracts/profile/messages/cordis/types';
import { createInterface } from 'node:readline';
import * as sdk from '../../src/index.ts';
import { ProbeExport } from '@cordis-x/test-contracts/profile';
for await (const line of createInterface({ input: process.stdin })) {
  const input = JSON.parse(line);
  let entered=0;
  const service=ProbeExport(()=>({
    async echo(_ctx,args){entered++;return args;},
    async number(_ctx,args){entered++;return args.value===-1n?create(Int64Schema):args;},
    async fail(_ctx,args){entered++;if(args.value==='refused')throw new sdk.Refusal('expected','runtime refusal');if(args.value==='unknown')throw new Error('unknown');throw new sdk.RpcError(args.value!,'business error');},
  }),{limit:10});
  const ctx={validation: 'audit'} as sdk.Context;await service.activate(ctx);
  let answer:Record<string,sdk.Value>;
  try {
    let signature;
    try { signature=operation(service.interface.methods,input.method); } catch(e) {throw new sdk.Refusal("unknown_method",String(e));}
    const codec=sdk.typed.protobuf(messageType(signature.args));
    let bytes:Uint8Array;
    try { bytes=codec.encode(codec.fromJson(input.args)); } catch(e) {throw new sdk.Refusal('invalid_argument',String(e));}
    const result=await service.invoke(input.method,bytes,ctx);
    const output=sdk.typed.protobuf(messageType(signature.result));
    answer={kind:'success',value:output.toJson(output.decode(result))};
  }
  catch(error){answer={kind:error instanceof sdk.Refusal?'refused':error instanceof sdk.RpcError?'rejected':'unknown'};}
  answer.entered=entered;console.log(JSON.stringify(answer));
}

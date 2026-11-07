import test from 'node:test';
import assert from 'node:assert/strict';
import { create } from '@bufbuild/protobuf';
import * as sdk from '../src/index.ts';
import { EchoClient as Public } from '../../../tests/generated/public_service/typescript/module.ts';
import { EchoClient as Local, EchoExport as local } from '../../../tests/generated/provider_local/typescript/module.ts';
import { EchoClient as Remote, EchoExport as remote } from '../../../tests/generated/provider_remote/typescript/module.ts';
import { ValueSchema } from '../../../tests/generated/public_service/typescript/proto/public_service_pb.ts';
test('public client calls providers with different private constructor shapes', async () => {
    assert.deepEqual(Public.interface(), Local.interface());
    assert.deepEqual(Public.interface(), Remote.interface());
    assert.deepEqual(Public.contract(), Local.contract());
    for (const [server, expected] of [
        [local(config => ({async get() {return create(ValueSchema,{text:config.directory});}}), {directory:'/objects'}), '/objects'],
        [remote(config => ({async get() {return create(ValueSchema,{text:config.endpoint});}}), {endpoint:'https://objects.invalid',tokenName:'alias'}), 'https://objects.invalid']
    ] as const) {
        const ctx = {validation:'checked', async invoke(_dependency, method, args) {return server.invoke(method,args,ctx);}} as sdk.Context;
        await server.activate(ctx);
        assert.equal((await new Public().get(ctx,create(ValueSchema))).text,expected);
        await server.deactivate(ctx);
    }
});

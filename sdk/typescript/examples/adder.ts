import { create } from '@bufbuild/protobuf';
import { AdderAddError, AdderExport } from '@cordis-x/example-contracts/examples';
import { Int64Schema } from '@cordis-x/example-contracts/examples/messages/cordis/types';
import { run } from '../src/adapters/process.ts';
const service = AdderExport(() => ({
    async add(_ctx, args) {
        const sum = args.left! + args.right!;
        if (sum < -(1n << 63n) || sum >= (1n << 63n))
            throw new AdderAddError('overflow', 'int64 addition overflow');
        return create(Int64Schema, { value: sum });
    },
}), {});
await run(service, { http: !process.argv.includes('--stdio-only') });

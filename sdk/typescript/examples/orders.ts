import { create } from '@bufbuild/protobuf';
import { EmptySchema } from '@bufbuild/protobuf/wkt';
import { OrdersExport, OrdersDependencies } from '@cordis-x/example-contracts/orders';
import { QuoteArgsSchema } from '@cordis-x/example-contracts/orders/messages/fixtures/pricing';
import { SubmitResultSchema } from '@cordis-x/example-contracts/orders/messages/fixtures/orders';
import { run } from '../src/adapters/process.ts';
await run(OrdersExport(() => ({
  async submit(ctx, args) {
    const quote = await OrdersDependencies.pricing().quote(ctx, create(QuoteArgsSchema, { quantity: args.quantity, unitPrice: args.unitPrice }));
    return create(SubmitResultSchema, {
      status: 'priced',
      quote: {
        total: quote.total,
        receipt: { generation: quote.receipt!.generation, owner: quote.receipt!.owner, caller: quote.receipt!.caller },
      },
    });
  },
  async deactivate(ctx) {
    await OrdersDependencies.pricing().flush(ctx, create(EmptySchema));
  },
}), {}));

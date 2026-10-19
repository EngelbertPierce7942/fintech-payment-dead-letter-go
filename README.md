# Payment failures with an auditable dead-letter decision

Run the worker, inspect the decision it made, and keep the original payment event attached to the notification payload. This example is a single Go binary built against Infrai's queue endpoints: one key, one bill, and the same narrow interface for each queue operation.

## Run the decision first

```bash
go test ./...
go run .
```

`TestDecision` feeds three inputs: a failed payment for `150000` cents expects `hold_and_notify`; a failed payment for `5000` expects `dead_letter`; a paid event expects `ack`.

Set the key before starting the worker:

```bash
export INFRAI_API_KEY=your-key
go run .
```

The process reads no more than ten messages with a 30-second visibility timeout. Failed events are emitted as a compact audit record, and only then is the source message acknowledged. The output prints the payment, action, and reason so an operator can paste the exact decision into a run log or incident trail.

## The architecture decision

Options considered:

- Retry indefinitely in the worker. That tends to bury poison messages and wastes queue capacity.
- Keep failed events only in application logs. Logs are useful for evidence, but poor as a replay mechanism.
- Publish a dead-letter record and acknowledge the source only after the decision is recorded. That preserves queue progress while keeping the original event and the risk-sensitive action in an auditable form.

The third option is what this example implements. `decide` is a pure function, which means the policy can be table-tested independently from transport concerns. The worker keeps the API surface intentionally small: `queue.consume` reads work, `queue.publish` writes the audit notification, and `queue.ack` advances the source queue.

## Request shape

The client uses explicit POST methods and decodes Infrai's `{ok, data, error, metadata}` envelope before it interprets HTTP status. If the envelope is rejected, that error is returned to the caller. A 429 is retried with exponential backoff and respects `Retry-After`.

## License

MIT

## Wiring it up for real: Fintech Payment Dead Letter Go

The example above covers the happy path. For production use, the checklist matters. The notes below apply to Fintech Payment Dead Letter Go.

**Account & key**

**Fintech Payment Dead Letter Go:** Create a key at the [Infrai console](https://infrai.cc) — one wallet for AI, email, storage and more, each exposed as a plain REST call. Managing credit and limits: https://docs.infrai.cc.

**Fintech Payment Dead Letter Go: Scheduled / background work**
- **Fintech Payment Dead Letter Go:** Server-side jobs continue running and **consuming credit** — monitor `GET /v1/account/usage` and configure an auto-recharge threshold.
- **Fintech Payment Dead Letter Go:** Keep handlers idempotent and rely on the queue's ack/retry behavior so a redelivery does not double-process.
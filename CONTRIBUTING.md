# Contributing to @pinceladasdaweb/rabbitmq

Thanks for taking the time. This library sits between an application and its
message broker, so most of its failure modes are silent: a consumer that stops
draining while publishing still looks healthy, a message acknowledged on the
wrong channel, a poison message requeued in a hot loop. The rules below exist
to keep those from shipping, not to be ceremony.

## Getting set up

```bash
git clone https://github.com/pinceladasdaweb/rabbitmq.git
cd rabbitmq
npm install
npm run hooks   # points core.hooksPath at .hooks — commit and branch checks
docker compose up -d   # RabbitMQ 4.2 with the management plugin, for the integration suite
```

Node 22 or newer. CI runs on 22 and 24.

| Command | What it does |
|---|---|
| `npm test` | The unit suite — no broker needed |
| `npm run test:coverage` | The same, with coverage |
| `npm run test:integration` | The same files against a real broker (`RABBITMQ_INTEGRATION=1`) |
| `npm run test:mutation` | [Stryker](https://stryker-mutator.io/) — grades whether the tests actually assert |
| `npx standard src/ tests/` | Lint; `npm run standard:fix` to autofix |
| `npm run check:types` | `tsc --noEmit --strict` over `index.d.ts` |
| `npm run build` | Dual ESM + CJS bundle plus the type declarations |

The integration suite needs the delayed-message plugin for one test and the
management API (port 15672) for the forced-disconnection tests. Both come with
the `docker compose` setup; see the README for installing the plugin.

## The rules that are not negotiable

**A mutation that survives is a missing test, not an acceptable number.**
Coverage says a line ran; it says nothing about whether anything asserted on
it. Every guard you add should come with a mutation that kills it: break the
line in `src`, confirm a test fails. This project has found real bugs that way
and lost a real one by ignoring it.

**Never delete defensive code because the reasoning says it is unreachable.**
If a mutant survives, decide which it is — a weak test or a genuinely
equivalent mutant — and say which in the pull request. A guard that cannot be
covered stays, with a comment explaining why. Removing one on the strength of
an argument once cost this project a duplicate-consumer bug where every message
was processed twice.

**Delivery tags are scoped to the channel that delivered them.** A message is
settled on its own channel or not at all. Falling back to a pool channel makes
the broker answer `PRECONDITION_FAILED` and close it, taking unrelated
in-flight publishes with it.

**Every nack path must be bounded.** Requeueing a message that will fail again
is an infinite loop at full speed. If a change can requeue, it has to explain
what stops the second attempt — today that is the `redelivered` flag behind
`retryPolicy`.

**Production code carries no test seams.** Tests reach the broker boundary by
replacing `amqp.connect` on the CommonJS module object amqplib exports (see
`tests/fake-amqp.js`), which leaves zero footprint in `src`. An injection point
added to `src` purely so a test can reach it is the wrong change.

**Errors are identified by `code`, never by message.** The RPC surface exposes
`RPC_TIMEOUT`, `RPC_CONNECTION_LOST`, `RPC_RESPONDER_ERROR` and
`RPC_UNROUTABLE`. Messages can be reworded; codes cannot.

**This package is published.** Any change to what an existing call does — not
just to a signature — is a behaviour change and needs the semver call made
explicitly in the pull request.

## Working on a change

Branches are prefixed with the type of change they carry, matching the commit
types: `feat/`, `fix/`, `docs/`, `test/`, `refactor/`, `perf/`, `build/`,
`ci/`, `chore/`, `style/`, `revert/`. The pre-push hook enforces it.

Commits follow [Conventional Commits](https://www.conventionalcommits.org/) —
commitlint checks the message. The release workflow reads the merge commit to
pick the version bump: a subject containing `minor` bumps the minor, one
containing `BREAKING CHANGE` or `major` bumps the major, anything else is a
patch. The CHANGELOG is generated from commit subjects, so write the subject
for the person reading the release notes.

Open pull requests against `development`, not `main`. Merging to `main`
publishes to npm.

## What a change needs before it lands

- **Tests that would fail without it.** Verify that by actually breaking the
  code and watching them fail — a test written after the fix often asserts
  nothing.
- **Both suites green.** Unit and integration. A change to reconnection,
  acknowledgement or consumer recovery is not demonstrated by unit tests alone.
- **Coverage and mutation score held.** `src` is above 99% of lines and
  functions. The number is the floor, not the goal.
- **Documentation.** New options go in the README with the trade-off spelled
  out, not just the signature. Anything users choose between deserves a
  runnable example under `examples/`.
- **Lint, types and build clean.**

## Reporting things

Bugs and feature requests go through the
[issue templates](https://github.com/pinceladasdaweb/rabbitmq/issues/new/choose).
Security vulnerabilities do **not** — see [SECURITY.md](SECURITY.md).

## Licence

Contributions are released under the [MIT licence](LICENSE) that covers the
project.

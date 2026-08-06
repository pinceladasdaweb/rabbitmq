# CHANGELOG

## 1.5.2 (2026-08-06)

* refactor: split the rate limiter into strategy objects with an injectable clock by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/58e041009bce5f5607590d0254efe4040bc810a7)
* test: extend the clock seam to SequentialProcessor and close every line-coverage gap by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/869d898f1aa4b30ebd08d75fa08a83d70001b13b)
* chore: declare log-line StringLiteral mutants out of the mutation gate by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/42ee5beb6a59294b50b0b35b0c3a5e83999d993a)
* fix: settle late successes, duplicate parked deliveries and non-Error publish failures by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/1bc52017abc0bee168e394518c548a3d5d54b488)
* refactor: finish the clock seam, share the window-log core, declare the log policy once by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/43279aa94a21a894f6bce20fd20ad3e88ab05065)
* test: hold the event loop open for unref'd-timer tests on Node 22 by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/c425579e53aea4fc0702b141e16f15b8084a9dc7)


## 1.5.1 (2026-08-02)

* fix: settle the message even when a handler throws a non-Error by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/b770912dfd465b83bb307ba7a138b5cec55bc4aa)


## 1.5.0 (2026-08-02)

* feat(minor): add mutation testing, community files and a configurable channel backoff by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/294f874027004c53ff7a9e8f7605bbd111004b48)


## 1.4.0 (2026-08-01)

* feat(minor): explicit retryPolicy option for every subscribe method by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/6b96cdc39ecb83096fffc84e1c715659579fde89)
* feat(minor): drop the deprecated setupGracefulShutdown alias and close the coverage gaps by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/94d492462cef15259d712c2513e06780b6312b4b)
* fix: make processDeadLetterQueue honor retryPolicy instead of ignoring it by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/80e100d08896876c7528d7f1e65cd9190b005974)
* fix(test): hold the event loop open in the facade's RPC timeout test by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/9c1fb48261cca9433f0a23b80eccb8c36b0b187e)


## 1.3.1 (2026-07-31)

* test: unit-cover topology and connection with injectable fakes by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/448b3aa6b921a0a4dd9e7e3cb9bff98506dc7620)
* test: unit-cover consumer-manager and publisher by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/75192e28352e113ff9c3a5af375ff5008a27d672)
* test: unit-cover the facade lifecycle and message codec by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/6bb249dabec31a65b0362cb8116188abf314c0da)
* fix: real poison-message policy for subscribeSequential, and honest tests by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/2775363897072a158f6f3b097a1389244e713f80)
* test: cover the remaining unguarded invariants; fix RPC reply-listener leak by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/ba24bcc07fcc6171c7efd0acb0f9654b8c062236)
* fix: settle messages only on their delivering channel by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/75877f863c83dd9d2e68bdf6bb10f66a9d8202d7)
* fix: restore full state when connect() beats the reconnection timer by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/a80752554fe97032157b1882b9ea063caa6d4f82)


## 1.3.0 (2026-07-30)

* feat(minor): request/response (RPC) support over direct reply-to by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/93c2c2889dfd52fe7c0b4756883ae7ed630050ad)
* fix(minor): harden RPC after adversarial review by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/0c9302e8c20e353422964320beffda0dd6fbfff3)
* fix: hold the event loop open in timeout-only RPC unit tests by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/ab018f2fb1b14592cb742b9cc0b0a5892f63833a)


## 1.2.1 (2026-07-29)

* chore(deps): upgrade packages by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/3c7e7241e62b3521bea1ce38e3be0b3c85cee6f7)


## 1.2.0 (2026-07-26)

* refactor: run publisher resilience on breakwater by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/71ec1edf44e675059c1a1087626d151121bb9c70)
* ci: create a GitHub release with changelog notes on publish by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/a9e33dac1c775c03767a92792806152a1c2eccab)


## 1.1.3 (2026-07-24)

* fix: friction-free install and dual-mode type declarations by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/bccdb4028aff9e2546bb2bab51e9ce785b24c72d)


## 1.1.2 (2026-07-23)

* chore: remove stale pino externals from rollup config by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/ce1572755aaf279086f84613f5ae02f0fb2e6bfa)


## 1.1.1 (2026-07-23)

* ci: migrate npm publishing to OIDC trusted publishing by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/d23cb0acf6c52a340be0b24a2a9278848abf11bf)


## 1.1.0 (2026-07-18)

* feat: replace default pino logger with a dependency-free console logger by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/469b24d727f647602c43e0e3945c29c08a67109f)


## 1.0.0 (2026-07-18)

* feat: initial release of the RabbitMQ client library by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/0f3db35e103ec5648096c9e521bf759430046888)
* fix: pin RabbitMQ 4.2 broker and matching delayed message plugin by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/daf1e011f2293485aff1d648ba02b7c980d2ab78)
* ci: grant explicit workflow permissions for release automation by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/58135c3e24ade16a6939b75c90a2c60bfbfd5500)

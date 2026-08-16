# CHANGELOG

## 1.7.0 (2026-08-16)

* build(deps): adopt breakwater 1.x and refresh the toolchain by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/2cca31ad03f9c4836a4309f948944cfc0a499eb9)
* feat: report each consumed message through per-message events (minor) by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/4e8db76cc20d7f9045321d621b91ffad84431dad)
* docs: map every failure mode to its behavior and its signal by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/3a8ff317e12a55e585b84d4eb07d150bb8a1d20d)
* ci: add Node 26 to the test matrix by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/67245d7256ed0d889ed740da1381904ae2c58873)
* fix: consumers survive connection outages of any length by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/989d2c31055d36883a6cf910b6d363647d727a0d)
* fix: event reporting can never interfere with message settlement by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/bf37385b5498e77f526bcc4af883b2150016f7ce)
* test: stop the suite leaking diagnostic reports and console noise by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/4de8ff2c01a669579ee89bfaf45319d702a996c4)
* fix: unsubscribe drains in-flight handlers before closing the channel by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/4e6b3d834d70ca022cd50102d669d25e5e5ec842)
* fix: one shared basic.return watcher, and mandatory is refused where it lies by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/5ad38d442628f307b689d1f6c1cea8f732efa96c)
* refactor: remove derivable state and shadow APIs the review flagged by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/79e2de376f60d4207ef9b62ab1fd140ca55e9e07)
* fix: restore the setupGracefulShutdown shim, and teach the gate Stryker 10 by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/31f01be4b7e5128921f189573d28507033ec7513)
* release: full-src review round 2 — consumer lifecycle, events and mandatory fixes (minor) by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/454c87aa6d686bd7d17eb9aa803a58b1afcd07b5)


## 1.6.0 (2026-08-08)

* fix: keep consumer tags valid across recreations and make the restore atomic by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/b992623be00734bdee7e03bc128b5d86198e5c8e)
* fix: recover consumers whose channel dies, and fence connect against shutdown by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/637d7db83e83e47e47358ca04c4f8e64d705a7c1)
* fix: give every consumer resource an owner and stop leaking channels by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/fcec1c1b90bf1c1dd61d763a8bfbe4548bbaf69e)
* ci: gate publishing on a green CI run instead of the push itself by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/8c2bf97f498f24b0cfbf8edffe0ef6bad508f846)
* fix: isolate the plugin probe, correlate DLQ returns and retry batches per message by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/55a527221139dfbe9b5c1fb88607fb435758e9cf)
* feat(minor): surface unroutable publishes and add a real retry budget by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/2336641c6b8c9d35d262903a6c934286c01f041e)
* fix: honour the retry budget on a quorum queue's FIRST delivery by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/6658eecca4f72bf2f18a02916dd01ab685b88fa9)
* test: close the coverage gaps and delete the dead branches behind them by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/b6f7916f76921f879b9388ba8e53c7a3f068bdb2)
* test: add a three-node cluster suite for failover behaviour by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/8cd679bc56143ac117a4977c7c2f6ef40d5982fa)
* test: kill the survivors left by today's new code by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/2b27697e468480b965cdbf75277c4cf5f30354ee)


## 1.5.3 (2026-08-07)

* perf: cut the full mutation run from 7 minutes to 4m15s by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/effdc4d9dc4a47b6207e7ce25336f279816e620f)
* test: clear the resilience/logger/codec mutation survivors; centralize the mutation policy by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/122dbf1fd2827f4f45634e4aafe41973b9152bb6)
* test: clear the sequential-processor, channel-pool and topology survivors by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/4972758064b48796a8621baa0c4995423f955356)
* test: clear the publisher and connection mutation survivors by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/afd7aeb123bf9f9abd8907ed6958c17fde11b8ab)
* test: clear the rpc and worker-pool mutation survivors; add the worker spawn seam by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/0215902c47c8a5b9abfbdfe6605bc77ca0e067d4)
* test: clear most consumer-manager mutation survivors (44 -> 14) by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/9a7a2dbbfa838bd4cd5dfe787e478034bf862504)
* test: keep the event loop alive for the new RPC suites on Node 22 by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/42597fbe4fa4815278e91014ecf43363fd4c3197)
* docs: ledger the last consumer-manager survivor as a proven equivalent by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/72ddbdefd8bfa32b6b777262ef45e15761c6a484)
* fix: fence the channel pool against a connection turnover mid-build by Pedro Rogério [View](https://github.com/pinceladasdaweb/rabbitmq/commit/8fe40404af515d922ee49b4c24c9ecbdffb7660c)


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

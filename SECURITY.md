# Security Policy

## Supported versions

Fixes land on the latest release. Please reproduce on the current version
before reporting.

| Version | Supported |
|---|---|
| Latest `1.x` | ✅ |
| Anything older | ❌ |

## Reporting a vulnerability

**Please do not open a public issue.**

Use GitHub's private vulnerability reporting:
[**Report a vulnerability**](https://github.com/pinceladasdaweb/rabbitmq/security/advisories/new).
It opens a private thread with the maintainer, and the fix and the advisory
can be prepared before anything becomes public.

Useful things to include: the affected version, a minimal reproduction, the
broker version, and what an attacker gains. A rough report sent early beats a
polished one sent late.

You can expect an acknowledgement within a few days. Once a fix is out, the
advisory is published and you are credited unless you would rather not be.

## What counts

This is a client library. It opens no listener of its own and reads no
configuration file — the credentials and endpoints come from the application.
The realistic reports are things like:

- Broker credentials or the connection string reaching somewhere they should
  not, including logs
- A message, its content or its acknowledgement crossing between consumers, or
  a reply from an RPC request resolving the wrong caller
- Input from a message body — headers, `messageId`, `depends-on` — that makes a
  consumer loop, allocate without bound, or leak channels, timers or memory
- Prototype pollution through an options object, a custom serializer or a
  message payload
- A supply-chain problem in the published artifact itself

Out of scope: vulnerabilities in your own handlers, in RabbitMQ itself, or in
your broker's configuration and network exposure. Also out of scope are scanner
reports against `devDependencies` that never ship — the published package
contains `dist` and the type declarations only.

There is no bug bounty.

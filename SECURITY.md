# Security Model

This document defines the trust assumptions, audit requirements, and delivery
guarantees of S3 MCP Relay. Review it before deploying the relay in a production
or shared environment.

## Trust boundaries

The controller host, agent host, and relay key are trusted. The S3 service and
network are not trusted with plaintext, but are trusted for availability.
Authenticated encryption detects modification and forgery; it does not prevent
deletion, delay, traffic analysis, rollback through retained versions, or denial
of service.

Compromise of the controller process or shared relay key permits an attacker to
create valid commands. Compromise of an agent permits forged responses for that
agent. Use separate S3 identities and a dedicated prefix and key for each
security domain to limit the impact of a credential or host compromise.

> [!WARNING]
> Full capability mode is remote code execution by design. Do not enable
> `allow_any_path` or `allow_any_program` on shared hosts or across unrelated
> trust domains.

## Logging and audit

Do not log payloads, standard output, standard error, file content, access keys,
or `RELAY_SHARED_KEY`. Infrastructure audit records should contain only the
principal, operation, object key, timestamp, result, and source network.

Associate production changes with the MCP client or user audit trail. The S3
object key contains a UUID and is not sufficient to attribute an operation to a
human identity.

## Delivery guarantee

S3 is object storage rather than a transactional queue. The relay deliberately
uses at-most-once command handoff to prevent automatic replay of side effects. A
failure between command deletion and response upload can therefore lose the
result even though the command ran successfully.

Exactly-once remote execution cannot be guaranteed without a durable,
agent-side idempotency journal or a reviewed transactional messaging service.
Before retrying a timed-out side-effecting command, verify its outcome with a
read-only operation.

## Storage requirements

- Use a dedicated bucket or prefix for each security domain.
- Disable versioning and Object Lock when deletion must remove relay objects.
- Enforce TLS through bucket policy.
- Apply a short lifecycle rule as a recovery safeguard.
- Do not replicate, archive, or log object payloads.
- Use separate controller and agent identities with least-privilege IAM policy.

## Vulnerability reports

Do not include credentials, relay keys, payloads, or sensitive infrastructure
details in a public issue. Use the repository's private security-reporting
channel when available, and provide only the minimum information required to
reproduce and assess the issue.

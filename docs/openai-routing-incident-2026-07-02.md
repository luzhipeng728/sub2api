# OpenAI account routing incident - 2026-07-02

## Summary

Production was rolled back from `da53169d` to `d7f8a662`.

The throughput drop was caused by the newer routing/TLS/transform changes after
`d7f8a662`, with the largest regression in `da53169d`: ordinary HTTP ingress
traffic stopped using the previously proven WSv2 bypass path unless the account
was near the 5h limit. That pushed most normal traffic onto the native HTTP/SSE
Codex path, where current large `/v1/responses` history payloads are not fully
compatible with Codex internal schema.

After rollback, traffic returned to the old WSv2 path and pool throughput
recovered.

## Version map

| Commit | Role | Status |
| --- | --- | --- |
| `d7f8a662` | Reverts the aggressive scheduler change. This is the stable rollback baseline and current production version after rollback. | Keep running tonight. |
| `5721d489` | Adds TLS fingerprint support to WS handshakes. | Part of risky change range. |
| `69b1cdb4` | Switches OpenAI WS TLS to a Codex rustls-style profile. | Introduced a real TLS curve issue before the next fix. |
| `d8f3efa1` | Removes the unsupported rustls PQ curve from the TLS profile. | Fixes the TLS curve error, but still contains TLS/WS fingerprint changes. |
| `da53169d` | Makes HTTP ingress bypass WSv2 only for accounts near 5h limit, and adds partial Codex input cleanup. | Regressed production throughput. Do not redeploy as-is. |

## Evidence

### Bad version: `da53169d`

Observed shortly before rollback:

```text
HEAD=da53169d
completed=904
assigned=758
200=847
403=1
502=56
forward_failed=51
content_unknown=80
tls_curve=0
decisions=257
http=250
ws=7
```

Key log pattern:

```text
Unknown parameter: 'input[31].content'
Unknown parameter: 'input[37].content'
```

Interpretation:

- TLS curve errors were already gone at this point (`tls_curve=0`), so TLS was
  not the active bottleneck after `d8f3efa1`.
- The transport decision changed heavily toward HTTP/SSE (`http=250`, `ws=7`).
- A meaningful share of `/v1/responses` requests then failed on Codex internal
  schema validation and surfaced as 502.

### Rollback/current production: `d7f8a662`

Immediately after rollback, before the later full middleware restart:

```text
HEAD=d7f8a662
starts=2402
completed=1924
assigned=1858
200=1906
400=16
403=0
429=0
502=2
decisions=89
http=0
ws=89
rpm_start=1601.3
rpm_done=1282.7
```

After the full service restart settled:

```text
HEAD=d7f8a662
starts=4632
completed=4622
assigned=4486
unique_accounts=41
200=4481
403=0
429=0
502=49
decisions=337
http=0
ws=337
rpm_start=1544.0
rpm_done=1540.7
```

Interpretation:

- The rollback restored the previous all-WSv2 HTTP ingress behavior.
- Account assignment recovered: 41 accounts were receiving traffic.
- 403 was not the main cause of the throughput collapse.
- Some schema errors still exist on the old path, but the failure rate is no
  longer pool-killing.

## Root cause

The regression is not a single TLS failure anymore. It is a routing and payload
compatibility problem introduced after `d7f8a662`.

`da53169d` changed HTTP ingress behavior so that normal OAuth accounts only use
WSv2 bypass when they are near the 5h limit. Accounts below the 5h threshold
fall back to the native HTTP/SSE path.

That was the wrong default for the current traffic mix because production
payloads include large `/v1/responses` histories with regular Responses API
items, for example:

- `item_...` ids
- `item_reference`
- non-message items carrying `content`
- tool/function call items whose ids should be `msg_...` or `fc_...`

Codex internal endpoints reject those shapes with errors like:

```text
Unknown parameter: input[x].content
Invalid input[x].id: item_... Expected an ID that begins with msg/fc
```

The partial cleanup in `da53169d` handled only part of this shape. It removed
some `item_...` ids and converted `content` for tool output items, but it did
not fully sanitize every non-message item that can appear in long Responses
history.

## What was not the main issue

- 403 was not the current bottleneck. During the bad sample it was `403=1`; after
  rollback it was `403=0`.
- Account acquisition was not completely broken. Requests did get assigned, but
  many failed or moved to a slower/less compatible path.
- TLS curve was a real earlier issue in `69b1cdb4`, but after `d8f3efa1` the
  observed `tls_curve` count was 0. TLS should still be reintroduced carefully,
  but it was not the active cause of the final low-rpm state.
- `BlockAccountScheduling`/aggressive scheduling was already reverted by
  `d7f8a662`; it was not the active current production version after rollback.

## Current production warning

Production is manually reset to `d7f8a662`, while the local branch and remote
branch still point at `da53169d`.

Do not run a normal production `git pull` from `feat/prewarm-session-bypass`
unless a fixed commit has replaced `da53169d`, otherwise production will move
back to the bad version.

## Recommended next steps

1. Keep production pinned to `d7f8a662` until a tested replacement is ready.
2. Add stage counters before the next rollout:
   - request accepted at HTTP entry
   - request body parsed / moderation started
   - account selected
   - upstream attempt started
   - upstream success/error
   - transport decision: HTTP/SSE vs WSv2
   - error class: 403, 429, 502, schema, TLS, auth, file download
3. Fix Codex request normalization with tests before changing routing again:
   - strip or rewrite `item_...` ids for Codex internal endpoints
   - remove `content` from non-message items unless the target schema allows it
   - convert tool output `content` into `output`
   - preserve required `call_id` context
4. Reintroduce TLS fingerprint behind a flag or canary account first.
5. Reintroduce the 5h warm-session behavior only after normal traffic can safely
   pass the same sanitizer.
6. Load-test with production-like large `/v1/responses` histories before
   redeploying.

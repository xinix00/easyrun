# cloudflared on HopOS — the example

A HopOS node behind NAT, with no inbound port and no kernel, made publicly
reachable by Cloudflare Tunnel. This is cloudflared's **own** `tunnel run`,
unmodified, running as a slot image on a dedicated core.

The point of this example is not the tunnel. It is that "just a Go program in a
slot" also holds for somebody else's Go program: cloudflared's entire dependency
tree — a forked quic-go, gopacket, gopsutil, gRPC, OpenTelemetry, Prometheus,
capnproto — compiles for `GOOS=tamago` untouched. Two small platform fallbacks
are all that is missing, and they are in [patch/](patch/).

```
30 MB  cloudflared-arm64-tamago.elf     (Pi 4/5, UEFI)
29 MB  cloudflared-riscv64-tamago.elf   (LicheeRV Nano — builds, but see Limits)
```

## Build it

```sh
tools/prepare-cloudflared.sh   # once, and after every version bump in go.mod
tools/build.sh                 # host tests + both slot images into out/
```

`prepare-cloudflared.sh` copies the pinned cloudflared out of the module cache
into `build/cloudflared-patched` (gitignored) and drops the two fallback files
in. **Until you run it, every go command in this module fails** with
"replacement directory ./build/cloudflared-patched does not exist" — including
`go test`. That is the trade for not maintaining a fork; see *Why a patch* below.

## Run it

The app dials **out**, so it publishes no port at all. Config comes from the job
spec env, under cloudflared's own env names — so the Cloudflare documentation
applies verbatim.

| env | what |
|---|---|
| `TUNNEL_TOKEN` | named tunnel; ingress comes from the Cloudflare dashboard. Empty ⇒ quick tunnel |
| `TUNNEL_URL` | the local service to expose. Default `http://$HOPOS_HOST` |
| `TUNNEL_TRANSPORT_PROTOCOL` | `http2` (our default) or `quic` |
| `TUNNEL_LOGLEVEL` | `info` by default |
| `CFD_EXTRA_ARGS` | free-form extra flags, split on spaces |

Plus the rest of [`cfd.Bridged`](internal/cfd/cfd.go): a slot's env is *not* the
process env (applib reads it from the control page), so the app copies a
documented allowlist into the process env with `os.Setenv`. applib cannot
enumerate its env, only look names up, so an allowlist is the only option —
extending it means adding a name there.

**Quick tunnel** — no account, no token, free `trycloudflare.com` hostname. With
no `TUNNEL_URL` it exposes `http://$HOPOS_HOST`, which is port 80 of this node:
by default this puts the [welcome](../welcome/) page on the public internet.

```json
{"name":"cloudflared","driver":"hop",
 "artifacts":[{"url":"https://…/cloudflared-arm64-tamago.elf"}],
 "memory_limit":268435456}
```

**Named tunnel** — your own hostname, ingress configured in the dashboard:

```json
{"name":"cloudflared","driver":"hop",
 "artifacts":[{"url":"https://…/cloudflared-arm64-tamago.elf"}],
 "memory_limit":268435456,
 "env":{"TUNNEL_TOKEN":"eyJhIjoi…"}}
```

`hop logs cloudflared` shows everything, the quick tunnel's public URL included.
That takes a trick: cloudflared logs with zerolog to `os.Stderr`, which on tamago
is the serial console and not HOP's log ring, so the app hangs an `os.Pipe` on
stdout/stderr and pumps it into `app.Logf` ([internal/cfd/pump.go](internal/cfd/pump.go)).
If `os.Pipe` is unavailable the app says so and keeps running — the logs stay on
the console.

## Why a patch, and why it is two files

cloudflared's `diagnostic` package (used by `tunnel diag`) only has
implementations for linux, darwin and windows: a traceroute collector that shells
out via `os/exec`, and a system collector that reads `/proc`, sysctl or WMI. On
any other platform those types simply do not exist and the package fails to
compile. HopOS has no `traceroute` binary, no `os/exec` and no host to interrogate
— the Go runtime *is* the OS — so the honest implementations are the ones in
[patch/](patch/): they return "not available on this platform". That is exactly
what an upstream PR would add.

Go's `-overlay` would be the clean way to inject them, but it refuses to touch
files under `GOMODCACHE` ("Files beneath GOMODCACHE must not be replaced"), hence
the copy-and-patch script. Alternatives, if this ever gets annoying: send the two
files upstream, or keep a fork and point the replace at it — one line in go.mod.

Note the other two replaces in [go.mod](go.mod): cloudflared has its **own**
replaces for `urfave/cli` and `quic-go`, and a dependency's replaces do not apply
transitively. Without repeating them, its code does not compile — it uses API
that only exists in those forks.

## What is verified, and what is not

Verified here: both images build and link; the generated command line is checked
against the **pinned** cloudflared's real flag tree, and every bridged env name
against its real `EnvVars`
([internal/cfd/flags_test.go](internal/cfd/flags_test.go)) — so an upstream rename
breaks a test instead of a node; the log pump, the mode/default logic and the
token never reaching a log line are host-tested.

**Not verified: an actual tunnel.** That needs a real node and a real Cloudflare
account, so treat the runtime as unproven. Known risks, in order:

1. **QUIC.** cloudflared defaults to it; we default to `http2` instead, because
   http2 is TCP+TLS over the same path as any other outbound connection from a
   slot. QUIC is UDP with socket options and datagram sizes that have to survive
   the per-slot gVisor netstack and HOP's masquerade. It may well work — nobody
   has measured it. `TUNNEL_TRANSPORT_PROTOCOL=quic` to try.
2. **Trust store.** There is no `/etc/ssl` on a slot, so `x509.SystemCertPool()`
   comes back empty and TLS to the edge would fail. The app imports
   `golang.org/x/crypto/x509roots/fallback` for the baked-in bundle — the same
   one the apploader uses to fetch artifacts over https.
3. **Filesystem and HOME.** With a token cloudflared needs no `cert.pem`, but
   parts of it still like a config directory. Nothing writes to disk in a slot.
4. **Clock.** TLS needs a correct one; HOP syncs via SNTP and applib takes the
   offset over at startup, so this should be fine.

## Limits

- **30 MB image** ⇒ `memory_limit` of ~256 MB. That is a Pi-class demo. The
  riscv64 build exists so the code keeps building for both, but a LicheeRV Nano
  has a 175 MB pool and one slot: it does not fit.
- Published to `rolling-release` like the other apps, so a job spec can point
  straight at it — but read *What is verified* above first: the tunnel itself has
  not been proven on a node yet.

  ```
  https://github.com/xinix00/hop/releases/download/rolling-release/cloudflared-arm64-tamago.elf
  https://github.com/xinix00/hop/releases/download/rolling-release/cloudflared-riscv64-tamago.elf
  ```

  `release.sh` runs `tools/prepare-cloudflared.sh` itself before the tamago
  builds (`HOPOS_PREPARE`), so a release picks up a version bump in go.mod
  without anyone remembering the step.
- `tunnel diag`, `tunnel login` and the service-install subcommands are along for
  the ride but cannot work here; the diagnostic collectors say so when asked.

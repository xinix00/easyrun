//go:build tamago

// cloudflared-hopos draait cloudflared's eigen tunnel-CLI als HopOS-slot-app:
// een node achter NAT, zonder inkomende poort, zonder kernel — en toch publiek
// bereikbaar. Dit is de voorbeeld-app die laat zien dat "gewoon een Go-programma
// in een slot" ook geldt voor programma's van iemand anders: alles behalve twee
// platform-fallbacks (zie ../../patch) bouwt ongewijzigd voor tamago.
//
// De app praat alleen naar buiten (de tunnel dialt uit), dus hij hoeft geen
// poort te publiceren. Config uit de jobspec-env, met cloudflared's eigen
// env-namen:
//
//	TUNNEL_TOKEN               named tunnel; ingress komt uit het dashboard
//	                           leeg = quick tunnel (gratis trycloudflare-URL)
//	TUNNEL_URL                 de lokale service; default http://$HOPOS_HOST
//	                           (= de gepubliceerde poort 80 van deze node)
//	TUNNEL_TRANSPORT_PROTOCOL  http2 (default) of quic
//	TUNNEL_LOGLEVEL            info (default)
//	CFD_EXTRA_ARGS             vrije extra flags, op spaties gesplitst
//
// Jobspec — quick tunnel die de welcome-pagina van deze node publiek maakt:
//
//	{"name":"cloudflared","driver":"hop","artifacts":[
//	  {"url":"…/cloudflared-arm64-tamago.elf","match":{"node.arch":"arm64"}},
//	  {"url":"…/cloudflared-riscv64-tamago.elf","match":{"node.arch":"riscv64"}}],
//	 "memory_limit":268435456}
//
// De URL waarop de node dan te bereiken is, staat in `hop logs cloudflared`.
package main

import (
	"log"
	"os"
	"runtime"
	"strings"

	"github.com/urfave/cli/v2"

	// Zonder systeem-trust-store (er is geen /etc/ssl op een slot) valt
	// x509.SystemCertPool leeg terug, en cloudflared verifieert de edge daarmee.
	// Deze import zet de meegebakken rootbundel als fallback — zelfde bundel
	// die de apploader gebruikt om artifacts van https te halen.
	_ "golang.org/x/crypto/x509roots/fallback"

	"hop-os/metal/app/applib"
	"hop-os/metal/app/applib/appnet"

	"github.com/cloudflare/cloudflared/cmd/cloudflared/tunnel"

	"cloudflared/internal/cfd"
)

var version = "dev" // -ldflags "-X main.version=vX.Y.Z"

// ringWriter stuurt stdlib-log naar de hop-ABI-logring (zelfde patroon als de
// satellieten). cloudflared zelf logt met zerolog naar stderr; dat vangt de
// pipe hieronder op.
type ringWriter struct{ app *applib.App }

func (w ringWriter) Write(p []byte) (int, error) {
	w.app.Logf("%s", strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

func main() {
	app := applib.Init() // eerste regel: READY + heartbeat + kill-flag
	log.SetFlags(0)      // de ring stempelt zelf
	log.SetOutput(ringWriter{app: app})

	ip, err := appnet.Up(app) // eigen TCP/IP-stack, eigen IP
	if err != nil {
		app.Logf("cloudflared: net: %v", err)
		app.Exit(1)
	}

	// stdout/stderr naar de logring, zodat cloudflared's eigen regels — de
	// quick-tunnel-URL incluis — in `hop logs cloudflared` staan. Lukt de pipe
	// niet, dan blijven ze op de console: hinderlijk, geen reden om niet te
	// draaien.
	if r, w, err := os.Pipe(); err == nil {
		os.Stdout, os.Stderr = w, w
		go cfd.Pump(r, app.Logf)
	} else {
		app.Logf("cloudflared: no pipe (%v) — cloudflared's own logs stay on the node console", err)
	}

	cfg := cfd.Load(app.Env)
	args, err := cfg.Args()
	if err != nil {
		app.Logf("cloudflared: %v", err)
		app.Exit(1)
	}
	if bridged := cfd.Bridge(app.Env, os.Setenv); len(bridged) > 0 {
		app.Logf("cloudflared: env from job spec: %s", strings.Join(bridged, " "))
	}

	app.Logf("%s", cfg.Banner(version, ip))
	app.Logf("cloudflared: slot %d, %d core(s), %s RAM, %s %s",
		app.Slot, runtime.NumCPU(), fmtMB(app.RAMSize), runtime.GOARCH, runtime.Version())

	// cloudflared's CLI leest os.Args, precies zoals op een gewone machine:
	// dezelfde subcommando's, dezelfde flags, dezelfde documentatie.
	os.Args = args
	cliApp := &cli.App{Name: "cloudflared", Version: version, Commands: tunnel.Commands()}
	app.Logf("cloudflared: exited: %v", cliApp.Run(os.Args))
	app.Exit(1) // een tunnel die stopt is een crash, by design: HOP herstart hem
}

// fmtMB houdt de logregel leesbaar zonder een formatter-pakket te linken.
func fmtMB(n uint64) string {
	const mb = 1 << 20
	if n < mb {
		return "<1 MB"
	}
	return strings.Join([]string{itoa(n / mb), "MB"}, " ")
}

func itoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

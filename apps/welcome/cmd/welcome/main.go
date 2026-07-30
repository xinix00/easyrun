//go:build tamago

// welcome is de app die een verse HopOS-node een gezicht geeft: één
// zelfstandige pagina op de gepubliceerde poort, zodat een headless node niet
// leeg aanvoelt maar vertelt dat hij leeft en wat er op hem draait. Hij staat
// in de standaard-hopos.init[] van de headless release.
//
// Alle zichtbare tekst (pagina én logs) is Engels — het is een publieke pagina.
//
// Config komt uit de jobspec-env; HOP zet ze allemaal zelf:
//
//	ER_PORT_HTTP  de gepubliceerde poort (uit ports:{http:...}); bind DIE
//	HOPOS_HOST    het node-IP waarop die poort van buiten open staat
//	HOP_DNS       de cluster-resolver (alleen om te tonen; niet gebruikt)
//
// Jobspec (zoals in image/hopos-headless.cfg):
//
//	hopos.init[]={"name":"welcome","driver":"hop",
//	 "artifacts":[{"url":"https://github.com/xinix00/hop/releases/download/rolling-release/welcome-arm64-tamago.elf"}],
//	 "memory_limit":67108864,
//	 "ports":{"http":80}}
package main

import (
	"runtime"

	"hop-os/metal/app/applib"
	"hop-os/metal/app/applib/apphttp"
	"hop-os/metal/app/applib/appnet"

	"welcome/internal/welcome"
)

var version = "dev" // -ldflags "-X main.version=vX.Y.Z"

func main() {
	app := applib.Init() // eerste regel: READY + heartbeat + kill-flag

	ip, err := appnet.Up(app) // eigen TCP/IP-stack, eigen IP
	if err != nil {
		app.Logf("welcome: net: %v", err)
		app.Exit(1)
	}

	// De poort die HOP publiceerde, nooit een hardgecodeerde 80: draait dit
	// image zonder publicatie (handmatig testen), dan is 8080 de terugval.
	port := app.Env("ER_PORT_HTTP")
	if port == "" {
		port = "8080"
	}

	srv := welcome.NewServer(welcome.Node{
		Host:    app.Env("HOPOS_HOST"),
		IP:      ip,
		Port:    port,
		DNS:     app.Env("HOP_DNS"),
		Slot:    app.Slot,
		Cores:   runtime.NumCPU(), // SMP zet HopOS zelf; hier alleen tellen
		RAMSize: app.RAMSize,      // de partitie die HOP gaf: harde grens
		Arch:    runtime.GOARCH,   // arm64 óf riscv64
		Runtime: runtime.Version(),
		Version: version,
	})

	app.Logf("%s", welcome.Banner(srv.Node()))
	app.Logf("http: %v", apphttp.ListenAndServe(":"+port, srv.Handle))
	app.Exit(1) // een service die stopt met serveren is een crash, by design
}

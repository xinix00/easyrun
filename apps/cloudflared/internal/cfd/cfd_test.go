package cfd

import (
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
)

// envOf maakt een App.Env-achtige lookup van een map.
func envOf(kv map[string]string) func(string) string {
	return func(k string) string { return kv[k] }
}

func TestLoadDefaultsToThePublishedPort(t *testing.T) {
	// Kale jobspec: geen token, geen URL. HOPOS_HOST is wat HOP altijd meegeeft,
	// dus de default is de gepubliceerde poort 80 van deze node — in de praktijk
	// de welcome-pagina.
	c := Load(envOf(map[string]string{"HOPOS_HOST": "10.0.0.7"}))
	if c.URL != "http://10.0.0.7" {
		t.Errorf("URL = %q, wil http://10.0.0.7", c.URL)
	}
	if c.Protocol != DefaultProtocol {
		t.Errorf("Protocol = %q, wil %q", c.Protocol, DefaultProtocol)
	}
	if c.LogLevel != DefaultLogLevel {
		t.Errorf("LogLevel = %q, wil %q", c.LogLevel, DefaultLogLevel)
	}
	if c.Named() || c.Mode() != "quick" {
		t.Errorf("zonder token is het een quick tunnel, kreeg mode %q", c.Mode())
	}
}

func TestLoadTrimsAndOverrides(t *testing.T) {
	c := Load(envOf(map[string]string{
		"TUNNEL_TOKEN":              "  tok123  ",
		"TUNNEL_URL":                " http://10.100.0.4:8080 ",
		"TUNNEL_TRANSPORT_PROTOCOL": "quic",
		"TUNNEL_LOGLEVEL":           "debug",
		"HOPOS_HOST":                "10.0.0.7",
		"CFD_EXTRA_ARGS":            "--edge-ip-version 6",
	}))
	if c.Token != "tok123" || c.URL != "http://10.100.0.4:8080" {
		t.Errorf("token/URL niet getrimd: %q / %q", c.Token, c.URL)
	}
	if c.Protocol != "quic" || c.LogLevel != "debug" {
		t.Errorf("overrides niet overgenomen: %q / %q", c.Protocol, c.LogLevel)
	}
	if len(c.Extra) != 2 || c.Extra[0] != "--edge-ip-version" {
		t.Errorf("Extra = %q", c.Extra)
	}
	if !c.Named() || c.Mode() != "named" {
		t.Errorf("met token is het een named tunnel, kreeg mode %q", c.Mode())
	}
}

func TestArgsQuickTunnel(t *testing.T) {
	c := Load(envOf(map[string]string{"HOPOS_HOST": "10.0.0.7"}))
	got, err := c.Args()
	if err != nil {
		t.Fatalf("Args: %v", err)
	}
	want := []string{"cloudflared", "tunnel", "--no-autoupdate",
		"--loglevel", "info", "--protocol", "http2", "--url", "http://10.0.0.7"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("Args =\n  %q\nwil\n  %q", got, want)
	}
}

func TestArgsNamedTunnelPassesTokenAndNoURL(t *testing.T) {
	// Bij een token bepaalt het dashboard de ingress; een --url erbij zou
	// suggereren dat wij dat doen.
	c := Load(envOf(map[string]string{"TUNNEL_TOKEN": "tok123", "HOPOS_HOST": "10.0.0.7"}))
	got, err := c.Args()
	if err != nil {
		t.Fatalf("Args: %v", err)
	}
	joined := strings.Join(got, " ")
	if !strings.Contains(joined, "run --token tok123") {
		t.Errorf("Args = %q, wil 'run --token tok123'", joined)
	}
	if strings.Contains(joined, "--url") {
		t.Errorf("Args bevat --url bij een named tunnel: %q", joined)
	}
	if !strings.Contains(joined, "--no-autoupdate") {
		t.Errorf("Args mist --no-autoupdate: %q", joined)
	}
}

func TestArgsAppendsExtraLast(t *testing.T) {
	c := Load(envOf(map[string]string{"HOPOS_HOST": "h", "CFD_EXTRA_ARGS": "--retries 3"}))
	got, _ := c.Args()
	if n := len(got); got[n-2] != "--retries" || got[n-1] != "3" {
		t.Errorf("Extra staat niet achteraan: %q", got)
	}
}

func TestArgsRejectsNonsense(t *testing.T) {
	// Geen doel en geen token: dat kan niet, en dat moet het zeggen i.p.v. een
	// cloudflared-hulptekst uit te spugen.
	if _, err := Load(envOf(nil)).Args(); !errors.Is(err, ErrNoTarget) {
		t.Errorf("zonder doel: err = %v, wil ErrNoTarget", err)
	}
	// Een onbekend protocol vangen we zelf: anders faalt het pas na het
	// opzetten van de netstack, in cloudflared's eigen flagparser.
	bad := Load(envOf(map[string]string{"HOPOS_HOST": "h", "TUNNEL_TRANSPORT_PROTOCOL": "sctp"}))
	if _, err := bad.Args(); err == nil {
		t.Error("protocol sctp werd geaccepteerd")
	}
	for _, p := range []string{"http2", "quic", "auto"} {
		ok := Load(envOf(map[string]string{"HOPOS_HOST": "h", "TUNNEL_TRANSPORT_PROTOCOL": p}))
		if _, err := ok.Args(); err != nil {
			t.Errorf("protocol %q afgewezen: %v", p, err)
		}
	}
}

func TestBannerNeverLeaksTheToken(t *testing.T) {
	c := Load(envOf(map[string]string{"TUNNEL_TOKEN": "supersecret", "HOPOS_HOST": "10.0.0.7"}))
	banner := c.Banner("v0.20.3", "10.100.0.5")
	if strings.Contains(banner, "supersecret") {
		t.Errorf("token staat in de banner: %q", banner)
	}
	for _, want := range []string{"named", "http2", "10.100.0.5", "dashboard"} {
		if !strings.Contains(banner, want) {
			t.Errorf("banner %q mist %q", banner, want)
		}
	}
	quick := Load(envOf(map[string]string{"HOPOS_HOST": "10.0.0.7"})).Banner("v0.20.3", "10.100.0.5")
	if !strings.Contains(quick, "http://10.0.0.7") {
		t.Errorf("quick-banner noemt het doel niet: %q", quick)
	}
}

func TestBridgeSetsOnlyWhatIsThere(t *testing.T) {
	set := map[string]string{}
	names := Bridge(
		envOf(map[string]string{"TUNNEL_TOKEN": "tok", "TUNNEL_LOGLEVEL": "debug", "IRRELEVANT": "x"}),
		func(k, v string) error { set[k] = v; return nil },
	)
	if len(names) != 2 {
		t.Errorf("gezette namen = %q, wil 2", names)
	}
	if set["TUNNEL_TOKEN"] != "tok" || set["TUNNEL_LOGLEVEL"] != "debug" {
		t.Errorf("proces-env = %v", set)
	}
	if _, ok := set["IRRELEVANT"]; ok {
		t.Error("Bridge zet dingen buiten de allowlist")
	}
	if _, ok := set["TUNNEL_URL"]; ok {
		t.Error("Bridge zet lege waarden")
	}
	// Een os.Setenv die faalt mag geen naam rapporteren (en niet panieken).
	if got := Bridge(envOf(map[string]string{"TUNNEL_TOKEN": "tok"}),
		func(string, string) error { return errors.New("nope") }); len(got) != 0 {
		t.Errorf("gefaalde set gerapporteerd als gezet: %q", got)
	}
}

func TestPumpForwardsLinesUntilEOF(t *testing.T) {
	// Dit is het pad waarlangs de quick-tunnel-URL in `hop logs` belandt.
	r, w := io.Pipe()
	var mu sync.Mutex
	var got []string
	done := make(chan struct{})
	go func() {
		Pump(r, func(format string, args ...any) {
			mu.Lock()
			got = append(got, args[0].(string))
			mu.Unlock()
		})
		close(done)
	}()

	io.WriteString(w, "INF Registered tunnel connection\n")
	io.WriteString(w, "\n") // lege regels overslaan
	io.WriteString(w, "INF https://random-words.trycloudflare.com\r\n")
	io.WriteString(w, "half line without newline")
	w.Close()
	<-done

	mu.Lock()
	defer mu.Unlock()
	want := []string{
		"INF Registered tunnel connection",
		"INF https://random-words.trycloudflare.com",
		"half line without newline",
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("Pump gaf\n  %q\nwil\n  %q", got, want)
	}
}

func TestPumpTruncatesAnOutlier(t *testing.T) {
	r, w := io.Pipe()
	var mu sync.Mutex
	var got string
	done := make(chan struct{})
	go func() {
		Pump(r, func(_ string, args ...any) {
			mu.Lock()
			got = args[0].(string)
			mu.Unlock()
		})
		close(done)
	}()
	io.WriteString(w, strings.Repeat("x", maxLogLine*3)+"\n")
	w.Close()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if len(got) > maxLogLine+32 {
		t.Errorf("regel niet afgekapt: %d bytes", len(got))
	}
	if !strings.HasSuffix(got, "(truncated)") {
		t.Error("afkappen wordt niet gemeld")
	}
}

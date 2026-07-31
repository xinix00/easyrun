package cfd

import (
	"bufio"
	"io"
	"strings"
)

// maxLogLine begrenst wat één regel mag worden in de logring. cloudflared logt
// zelf nette regels; dit is er tegen een enkele uitschieter (een dump van een
// edge-antwoord bijvoorbeeld) die anders de ring vol duwt.
const maxLogLine = 4 << 10

// Pump leest regels van r en geeft ze door aan logf tot r op is.
//
// Waarom dit bestaat: cloudflared logt met zerolog naar os.Stderr, en dat is op
// tamago de seriële console — niet HOP's logring. Juist de interessantste regel
// van een quick tunnel (de trycloudflare-URL waarop je node te bereiken is)
// staat daar tussen, en die wil je in `hop logs cloudflared` zien en niet aan
// een console hangen. De main hangt daarom een os.Pipe aan os.Stderr en laat
// deze functie de leeskant doorgeven.
func Pump(r io.Reader, logf func(string, ...any)) {
	br := bufio.NewReaderSize(r, maxLogLine)
	for {
		line, err := br.ReadString('\n')
		line = strings.TrimRight(line, "\r\n")
		if len(line) > maxLogLine {
			line = line[:maxLogLine] + " …(truncated)"
		}
		if line != "" {
			logf("%s", line)
		}
		if err != nil {
			return // EOF of een dichte pipe: klaar
		}
	}
}

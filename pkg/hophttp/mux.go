package hophttp

// mux.go — the router, and it is ours on both platforms on purpose.
//
// hop's routes need three things that a prefix switch cannot express: a method
// in the pattern ("GET /v1/agents"), single-segment wildcards that the handler
// reads back ("/v1/agents/{agent_id}/capacity"), and subtrees ("/logs/"). They
// also need precedence: /v1/agents/ and /v1/agents/{id}/logs/ both match a log
// request, and the second one has to win, or every log tail gets answered by
// the buffered proxy and nothing ever streams.
//
// net/http's ServeMux does all of that. leanhttp has no mux at all (surfserve
// hand-rolled a switch). Using each platform's own would mean the node routes
// by one set of precedence rules and the machine it was developed on by
// another — a difference that only shows up in the corner cases nobody tests.
// So there is one mux, here, and both transports feed it.

import (
	"sort"
	"strings"
)

// Mux routes requests to handlers by method, path and pattern specificity.
// The zero value is not usable; call NewServeMux.
type Mux struct {
	routes []route
}

// route is one registered pattern, pre-parsed so matching does no allocation
// beyond the wildcard map (and that one only when the pattern has wildcards).
type route struct {
	method  string   // "" = any method
	lits    []string // per segment: the literal, or "" when it is a wildcard
	names   []string // per segment: the wildcard name, or "" when it is a literal
	subtree bool     // pattern ended in "/": also matches everything below
	h       Handler
	score   int
}

// NewServeMux returns an empty Mux.
func NewServeMux() *Mux { return &Mux{} }

// HandleFunc registers h for pattern. The pattern is
//
//	[METHOD ]/path[/]
//
// where a segment written as {name} matches exactly one non-empty segment and
// is readable through [Request.PathValue], and a trailing "/" makes the pattern
// match everything below it as well.
//
// Registering the same pattern twice, or a pattern without a leading "/",
// panics. That is deliberate: both are wiring mistakes, they are always
// present from the first run, and a route that silently never fires is the
// worst way to find out.
func (m *Mux) HandleFunc(pattern string, h Handler) {
	method, path := "", pattern
	if i := strings.IndexByte(pattern, ' '); i >= 0 {
		method, path = pattern[:i], strings.TrimSpace(pattern[i+1:])
	}
	if !strings.HasPrefix(path, "/") {
		panic("hophttp: pattern " + pattern + " must start with /")
	}
	for _, r := range m.routes {
		if r.method == method && r.pattern() == path {
			panic("hophttp: duplicate pattern " + pattern)
		}
	}

	rt := route{method: method, h: h, subtree: strings.HasSuffix(path, "/")}
	// A subtree's trailing empty segment is not a segment to match.
	for _, seg := range strings.Split(strings.Trim(path, "/"), "/") {
		if seg == "" {
			continue // the root pattern "/" has no segments
		}
		if len(seg) > 2 && seg[0] == '{' && seg[len(seg)-1] == '}' {
			rt.lits = append(rt.lits, "")
			rt.names = append(rt.names, seg[1:len(seg)-1])
			rt.score += 1 // a wildcard is a match, but the weakest kind
			continue
		}
		rt.lits = append(rt.lits, seg)
		rt.names = append(rt.names, "")
		rt.score += 4 // a literal segment beats a wildcard at the same depth
	}
	if !rt.subtree {
		rt.score += 2 // an exact pattern beats a subtree that reaches as deep
	}
	if method != "" {
		rt.score++ // a pattern that names its method beats one that takes any
	}

	m.routes = append(m.routes, rt)
	// Most specific first, so the first match is the right one. Sorted at
	// registration (which happens once) instead of per request.
	sort.SliceStable(m.routes, func(i, j int) bool { return m.routes[i].score > m.routes[j].score })
}

// pattern rebuilds the path form of a route, for the duplicate check and for
// error messages.
func (r route) pattern() string {
	var b strings.Builder
	for i, lit := range r.lits {
		b.WriteByte('/')
		if lit == "" {
			b.WriteString("{" + r.names[i] + "}")
			continue
		}
		b.WriteString(lit)
	}
	if r.subtree {
		b.WriteByte('/')
	}
	if b.Len() == 0 {
		return "/"
	}
	return b.String()
}

// ServeHTTP is the Mux as a Handler, so it can be wrapped in middleware like
// any other.
func (m *Mux) ServeHTTP(w ResponseWriter, r *Request) {
	segs := splitPath(r.Path)
	pathMatched := false
	for i := range m.routes {
		rt := &m.routes[i]
		vals, ok := rt.match(segs)
		if !ok {
			continue
		}
		if rt.method != "" && rt.method != r.Method {
			pathMatched = true // the path exists, the method does not
			continue
		}
		r.vals = vals
		rt.h(w, r)
		return
	}
	if pathMatched {
		Error(w, "method not allowed", StatusMethodNotAllowed)
		return
	}
	Error(w, "not found", StatusNotFound)
}

// Handler returns the mux as a plain Handler.
func (m *Mux) Handler() Handler { return m.ServeHTTP }

// match tests one route against the request's segments and returns the
// wildcard values it captured.
func (r *route) match(segs []string) (map[string]string, bool) {
	switch {
	case r.subtree && len(segs) < len(r.lits):
		return nil, false
	case !r.subtree && len(segs) != len(r.lits):
		return nil, false
	}
	var vals map[string]string
	for i, lit := range r.lits {
		if lit == "" { // wildcard: one non-empty segment, any content
			if segs[i] == "" {
				return nil, false
			}
			if vals == nil {
				vals = make(map[string]string, len(r.lits))
			}
			vals[r.names[i]] = segs[i]
			continue
		}
		if segs[i] != lit {
			return nil, false
		}
	}
	return vals, true
}

// splitPath cuts a path into its segments. "/" and "" both give none, so the
// root pattern matches them both.
func splitPath(p string) []string {
	p = strings.Trim(p, "/")
	if p == "" {
		return nil
	}
	return strings.Split(p, "/")
}

package policy

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Config is handed to every policy constructor.
type Config struct {
	// CacheSize is the cache capacity in bytes. Policies that size internal
	// structures as a fraction of the cache (S3-FIFO's small queue, for
	// example) need it; most ignore it.
	CacheSize int64

	// Seed drives any randomness a policy uses. It always comes from an
	// explicit flag with a fixed default, so runs are reproducible.
	Seed int64

	// Params is the raw policy-specific parameter string, parsed with
	// ParseParams.
	Params string
}

type (
	// Constructor builds an online policy.
	Constructor func(Config) (Policy, error)
	// OracleConstructor builds an offline policy that sees next-access times.
	OracleConstructor func(Config) (OraclePolicy, error)
)

// The two registries are kept apart so that looking up an online policy can
// never return something that expects future information.
var (
	online = map[string]Constructor{}
	oracle = map[string]OracleConstructor{}
)

// Register adds an online policy. It panics on a duplicate or on a name already
// registered as an oracle, since both are programmer errors at init time.
func Register(name string, c Constructor) {
	name = strings.ToLower(name)
	if _, dup := online[name]; dup {
		panic("policy: duplicate registration " + name)
	}
	if _, dup := oracle[name]; dup {
		panic("policy: " + name + " is already registered as an oracle policy")
	}
	online[name] = c
}

// RegisterOracle adds an offline policy that requires next-access times.
func RegisterOracle(name string, c OracleConstructor) {
	name = strings.ToLower(name)
	if _, dup := oracle[name]; dup {
		panic("policy: duplicate oracle registration " + name)
	}
	if _, dup := online[name]; dup {
		panic("policy: " + name + " is already registered as an online policy")
	}
	oracle[name] = c
}

// New builds an online policy by name. Asking for an oracle policy here is an
// error rather than a silent fallback: it is the check behind the replayer's
// refusal to run Belady without --oracle.
func New(name string, cfg Config) (Policy, error) {
	name = strings.ToLower(name)
	if c, ok := online[name]; ok {
		return c(cfg)
	}
	if _, ok := oracle[name]; ok {
		return nil, fmt.Errorf("policy %q is an offline oracle policy and requires --oracle", name)
	}
	return nil, fmt.Errorf("unknown policy %q (available: %s)", name, strings.Join(AllNames(), ", "))
}

// NewOracle builds an offline policy by name.
func NewOracle(name string, cfg Config) (OraclePolicy, error) {
	name = strings.ToLower(name)
	if c, ok := oracle[name]; ok {
		return c(cfg)
	}
	if _, ok := online[name]; ok {
		return nil, fmt.Errorf("policy %q is an online policy and must not be run with --oracle", name)
	}
	return nil, fmt.Errorf("unknown oracle policy %q (available: %s)", name, strings.Join(OracleNames(), ", "))
}

// IsOracle reports whether name is registered as an offline oracle policy.
func IsOracle(name string) bool {
	_, ok := oracle[strings.ToLower(name)]
	return ok
}

// IsKnown reports whether name is registered at all.
func IsKnown(name string) bool {
	name = strings.ToLower(name)
	_, on := online[name]
	_, or := oracle[name]
	return on || or
}

// Names lists registered online policies, sorted. Sorting matters: map
// iteration order is unspecified, and this list reaches both help text and
// results files.
func Names() []string { return sortedKeys(online) }

// OracleNames lists registered oracle policies, sorted.
func OracleNames() []string { return sortedKeysOracle(oracle) }

// AllNames lists every registered policy, sorted.
func AllNames() []string {
	all := append(Names(), OracleNames()...)
	sort.Strings(all)
	return all
}

func sortedKeys(m map[string]Constructor) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedKeysOracle(m map[string]OracleConstructor) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Params is a parsed policy parameter string.
type Params map[string]string

// ParseParams parses "key=value,key=value". An empty string yields empty params.
func ParseParams(s string) (Params, error) {
	p := Params{}
	if strings.TrimSpace(s) == "" {
		return p, nil
	}
	for _, tok := range strings.Split(s, ",") {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		k, v, ok := strings.Cut(tok, "=")
		if !ok {
			return nil, fmt.Errorf("policy params: %q is not key=value", tok)
		}
		p[strings.TrimSpace(strings.ToLower(k))] = strings.TrimSpace(v)
	}
	return p, nil
}

// Float returns a float parameter, or def if absent.
func (p Params) Float(key string, def float64) (float64, error) {
	v, ok := p[key]
	if !ok {
		return def, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("policy params: %s=%q must be a number", key, v)
	}
	return f, nil
}

// Int returns an integer parameter, or def if absent.
func (p Params) Int(key string, def int) (int, error) {
	v, ok := p[key]
	if !ok {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("policy params: %s=%q must be an integer", key, v)
	}
	return n, nil
}

// Sorted renders the params as a stable "k=v,k=v" string for results files, so
// that a recorded run states the parameters it actually used.
func (p Params) Sorted() string {
	keys := make([]string, 0, len(p))
	for k := range p {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + "=" + p[k]
	}
	return strings.Join(parts, ",")
}

// Unknown reports any parameter keys not in allowed, so a typo in a swept
// parameter fails loudly instead of silently using a default.
func (p Params) Unknown(allowed ...string) error {
	ok := make(map[string]bool, len(allowed))
	for _, a := range allowed {
		ok[a] = true
	}
	var bad []string
	for k := range p {
		if !ok[k] {
			bad = append(bad, k)
		}
	}
	if len(bad) == 0 {
		return nil
	}
	sort.Strings(bad)
	return fmt.Errorf("unknown policy params: %s (accepted: %s)", strings.Join(bad, ", "), strings.Join(allowed, ", "))
}

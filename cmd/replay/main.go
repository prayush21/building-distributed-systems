// Command replay drives a cache policy over a trace and prints metrics as JSON.
//
//	replay --trace PATH --policy lru --size 1gb [--ignore-obj-size]
//
// Column mapping follows libCacheSim's --trace-format-params, with 1-indexed
// column numbers, so the same trace and arguments can be handed to cachesim for
// cross-checking.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/prayush21/building-distributed-systems/internal/policy"
	"github.com/prayush21/building-distributed-systems/internal/replay"
	"github.com/prayush21/building-distributed-systems/internal/trace"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "replay:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		tracePath    = flag.String("trace", "", "path to the trace file (.csv, .csv.gz, or .zst)")
		policyName   = flag.String("policy", "lru", "cache policy name")
		policyParams = flag.String("policy-params", "", "policy-specific parameters, \"k=v,k=v\"")
		sizeSpec     = flag.String("size", "", "cache size: 1gb, 100mb, a raw byte count, or 0.1x of the working set")
		ignoreSize   = flag.Bool("ignore-obj-size", false, "treat every object as size 1, so --size is a count of objects (libCacheSim semantics)")
		warmupFrac   = flag.Float64("warmup-frac", 0, "fraction of the trace used to populate the cache and excluded from metrics")
		seed         = flag.Int64("seed", 1, "seed for any policy randomness; fixed by default so runs reproduce")
		oracle       = flag.Bool("oracle", false, "permit offline policies that see future accesses (Belady)")
		maxRequests  = flag.Int64("max-requests", 0, "stop after this many requests, 0 for the whole trace")
		formatParams = flag.String("trace-format-params", "", "libCacheSim-style column mapping, e.g. \"time-col=1,obj-id-col=2,obj-size-col=3\"")
		compact      = flag.Bool("compact", false, "emit single-line JSON")
		listPolicies = flag.Bool("list-policies", false, "list registered policies and exit")
	)
	flag.StringVar(formatParams, "t", "", "alias for --trace-format-params")

	flag.Usage = usage
	flag.Parse()

	if *listPolicies {
		fmt.Println("online policies:", strings.Join(policy.Names(), ", "))
		if names := policy.OracleNames(); len(names) > 0 {
			fmt.Println("oracle policies (need --oracle):", strings.Join(names, ", "))
		}
		return nil
	}

	if *tracePath == "" {
		return fmt.Errorf("--trace is required")
	}
	if *sizeSpec == "" {
		return fmt.Errorf("--size is required")
	}

	size, err := replay.ParseSize(*sizeSpec)
	if err != nil {
		return err
	}
	csvParams, err := trace.ParseCSVParams(*formatParams)
	if err != nil {
		return err
	}

	// Refuse an oracle policy without the flag here as well as in the
	// registry, so the reason reaches the user before a trace is read.
	if policy.IsOracle(*policyName) && !*oracle {
		return fmt.Errorf("%q is an offline policy that sees future accesses; pass --oracle to run it as a baseline", *policyName)
	}

	res, err := replay.Run(replay.Options{
		TracePath:     *tracePath,
		Policy:        *policyName,
		PolicyParams:  *policyParams,
		Size:          size,
		IgnoreObjSize: *ignoreSize,
		WarmupFrac:    *warmupFrac,
		Seed:          *seed,
		Oracle:        *oracle,
		MaxRequests:   *maxRequests,
		CSVParams:     csvParams,
	})
	if err != nil {
		return err
	}

	enc := json.NewEncoder(os.Stdout)
	if !*compact {
		enc.SetIndent("", "  ")
	}
	if err := enc.Encode(res); err != nil {
		return err
	}

	// A policy that named victims it did not hold produced numbers that mean
	// nothing. Say so on stderr and fail, rather than letting a sweep record
	// a plausible-looking miss ratio.
	if res.Metrics.PolicyErrors > 0 {
		return fmt.Errorf("policy %q reported %d contract violations; results are invalid",
			res.Run.Policy, res.Metrics.PolicyErrors)
	}
	return nil
}

func usage() {
	out := flag.CommandLine.Output()
	fmt.Fprintf(out, `replay drives a cache policy over a trace and prints metrics as JSON.

Usage:
  replay --trace PATH --policy lru --size 1gb [--ignore-obj-size]

Examples:
  replay --trace data/trace.csv --policy lru --size 1gb
  replay --trace data/trace.csv --policy lru --size 0.1x --warmup-frac 0.2
  replay --trace data/trace.zst --policy lru --size 1000 --ignore-obj-size \
      -t "time-col=1,obj-id-col=2,obj-size-col=4,has-header=true"

Flags:
`)
	flag.PrintDefaults()
}

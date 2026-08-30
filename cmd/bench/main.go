// Command bench sweeps cache policies across a directory of traces at a range
// of cache sizes, and emits both JSON and a markdown table.
//
//	bench --traces DIR --policies fifo,lru,sieve --sizes 0.01x,0.1x
//
// It reports raw miss ratios, miss ratio reduction against FIFO, and the share
// of the LRU-to-Belady gap each policy closed. The derived columns exist
// because raw miss ratios are not comparable across traces.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/prayush21/building-distributed-systems/internal/bench"
	"github.com/prayush21/building-distributed-systems/internal/policy"
	"github.com/prayush21/building-distributed-systems/internal/replay"
	"github.com/prayush21/building-distributed-systems/internal/trace"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		traceDir  = flag.String("traces", "", "directory of trace files to sweep")
		traceGlob = flag.String("trace-glob", "", "glob selecting trace files, for layouts --traces does not match")
		policies  = flag.String("policies", "fifo,lru,lfu,s3fifo,sieve,belady",
			"policies to compare; append \":k=v\" for parameters, e.g. s3fifo:small-size-ratio=0.2")
		sizes = flag.String("sizes", "0.001x,0.01x,0.05x,0.1x",
			"cache sizes to sweep: 1gb, 100mb, a byte count, or a fraction of the working set")
		ignoreSize   = flag.Bool("ignore-obj-size", false, "treat every object as size 1, so sizes are object counts")
		warmupFrac   = flag.Float64("warmup-frac", 0, "fraction of each trace used to populate the cache and excluded from metrics")
		seed         = flag.Int64("seed", 1, "seed for any policy randomness")
		oracle       = flag.Bool("oracle", true, "include the offline Belady baseline; required for the gap-closed column")
		maxRequests  = flag.Int64("max-requests", 0, "stop each trace after this many requests, 0 for all")
		formatParams = flag.String("trace-format-params", "", "libCacheSim-style column mapping")
		jsonOut      = flag.String("json", "", "write JSON results to this path")
		mdOut        = flag.String("markdown", "", "write the markdown table to this path (default stdout)")
	)
	flag.StringVar(formatParams, "t", "", "alias for --trace-format-params")
	flag.Usage = usage
	flag.Parse()

	if *traceDir == "" && *traceGlob == "" {
		return fmt.Errorf("one of --traces or --trace-glob is required")
	}
	if *traceDir != "" && *traceGlob != "" {
		return fmt.Errorf("--traces and --trace-glob are mutually exclusive")
	}

	specs, err := bench.ParseSpecs(*policies)
	if err != nil {
		return err
	}
	for _, s := range specs {
		if !policy.IsKnown(s.Name) {
			return fmt.Errorf("unknown policy %q (available: %s)", s.Name,
				strings.Join(policy.AllNames(), ", "))
		}
	}

	var parsed []replay.Size
	for _, spec := range strings.Split(*sizes, ",") {
		spec = strings.TrimSpace(spec)
		if spec == "" {
			continue
		}
		size, err := replay.ParseSize(spec)
		if err != nil {
			return err
		}
		parsed = append(parsed, size)
	}

	csvParams, err := trace.ParseCSVParams(*formatParams)
	if err != nil {
		return err
	}
	var paths []string
	if *traceGlob != "" {
		paths, err = bench.MatchTraces(*traceGlob)
	} else {
		paths, err = bench.FindTraces(*traceDir)
	}
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "bench: %d traces x %d sizes x %d policies = %d runs\n",
		len(paths), len(parsed), len(specs), len(paths)*len(parsed)*len(specs))

	rep, err := bench.Run(bench.Options{
		TracePaths:    paths,
		Specs:         specs,
		Sizes:         parsed,
		IgnoreObjSize: *ignoreSize,
		WarmupFrac:    *warmupFrac,
		Seed:          *seed,
		MaxRequests:   *maxRequests,
		CSVParams:     csvParams,
		Oracle:        *oracle,
	})
	if err != nil {
		return err
	}

	if *jsonOut != "" {
		b, err := json.MarshalIndent(rep, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(*jsonOut, append(b, '\n'), 0o644); err != nil {
			return err
		}
		fmt.Fprintln(os.Stderr, "bench: wrote", *jsonOut)
	}

	md := bench.Markdown(rep)
	if *mdOut == "" {
		fmt.Print(md)
	} else {
		if err := os.WriteFile(*mdOut, []byte(md), 0o644); err != nil {
			return err
		}
		fmt.Fprintln(os.Stderr, "bench: wrote", *mdOut)
	}

	// A cell that could not be configured is a normal outcome and is reported
	// in the table. A cell whose policy violated its contract means the numbers
	// beside it are meaningless, so the sweep fails.
	if invalid := rep.Invalid(); len(invalid) > 0 {
		for _, c := range invalid {
			fmt.Fprintf(os.Stderr, "bench: INVALID %s %s %s: %s\n",
				c.Trace, c.SizeSpec, c.Policy, c.Error)
		}
		return fmt.Errorf("%d configurations produced invalid results", len(invalid))
	}
	return nil
}

func usage() {
	out := flag.CommandLine.Output()
	fmt.Fprintf(out, `bench sweeps cache policies across traces and emits JSON and a markdown table.

Usage:
  bench --traces DIR [--policies ...] [--sizes ...]

Examples:
  bench --traces harness/traces --json results.json --markdown results.md
  bench --traces harness/traces --sizes 0.01x,0.1x --warmup-frac 0.2
  bench --traces harness/traces --policies "lru,s3fifo,s3fifo:small-size-ratio=0.2"
  bench --traces harness/traces --sizes 1000,10000 --ignore-obj-size
  bench --trace-glob 'data/*.oracleGeneral.csv' --json results.json

Flags:
`)
	flag.PrintDefaults()
}

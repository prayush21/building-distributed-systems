# Building Distributed Systems

## Cache policy harness

`internal/policy` implements the eviction policies; `internal/replay` drives
one policy over a trace and reports miss ratios and cost metrics;
`internal/bench` sweeps many policies/sizes across a directory of traces.

- [`cmd/replay`](cmd/replay/main.go) — run a single policy over a single trace.
- [`cmd/bench`](cmd/bench/main.go) — sweep policies × cache sizes across a
  directory of traces, emitting JSON and a markdown report.

```bash
go build -o /tmp/replay ./cmd/replay
/tmp/replay --trace harness/traces/alibabaBlock_70.csv --policy sieve --size 0.05x \
  -t 'time-col=1,obj-id-col=2,obj-size-col=3,has-header=false'
```

```bash
go build -o /tmp/bench ./cmd/bench
/tmp/bench --traces harness/traces --warmup-frac 0.2 \
  -t 'time-col=1,obj-id-col=2,obj-size-col=3,has-header=false' \
  --json harness/benchmark.json --markdown harness/BENCHMARK.md
```

Results across the cacheMon Alibaba block traces are in
[harness/BENCHMARK.md](harness/BENCHMARK.md).

### Validation against libCacheSim

The replayer's FIFO/LRU/LFU/S3-FIFO/SIEVE/Belady output is cross-checked
against libCacheSim on the same traces and cache sizes — currently passing
72/72 points within 0.001 absolute miss ratio. See
[harness/VALIDATION.md](harness/VALIDATION.md) for the full report,
reproduction steps, and what the gate has caught.

```bash
pip install libcachesim
harness/fetch_traces.sh harness/traces
go build -o /tmp/replay ./cmd/replay
python3 harness/validate.py --trace-dir harness/traces --replay /tmp/replay
```

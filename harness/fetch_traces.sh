#!/usr/bin/env bash
# Fetch small traces from the cacheMon cache_dataset for validation.
#
#   harness/fetch_traces.sh [dest-dir]
#
# Defaults to harness/traces, which is gitignored: these are tens of megabytes
# and are reproducible from this script, so they do not belong in the repo.
#
# The alibabaBlock 100K set is used because its traces are small enough to
# validate against quickly while still being real workloads with skewed,
# genuinely variable object sizes. That last property matters: 63% of the
# objects in these traces appear at more than one size, which is what exposed
# the object-size handling bug this validation caught.
set -euo pipefail

DEST="${1:-harness/traces}"
BASE="https://cache-datasets.s3.amazonaws.com/cache_dataset_oracleGeneral/2020_alibabaBlock/100K"
TRACES=(alibabaBlock_70 alibabaBlock_98 alibabaBlock_548)

command -v zstd >/dev/null || { echo "zstd is required (brew install zstd)" >&2; exit 1; }
mkdir -p "$DEST"

for t in "${TRACES[@]}"; do
  if [[ -f "$DEST/$t.oracleGeneral" ]]; then
    echo "have $t"
    continue
  fi
  echo "fetching $t"
  curl -fsSL --retry 3 -o "$DEST/$t.oracleGeneral.zst" "$BASE/$t.oracleGeneral.zst"
  zstd -dqf "$DEST/$t.oracleGeneral.zst" -o "$DEST/$t.oracleGeneral"
  rm -f "$DEST/$t.oracleGeneral.zst"
done

echo
echo "traces in $DEST:"
ls -lh "$DEST"/*.oracleGeneral

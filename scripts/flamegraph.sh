#!/usr/bin/env bash
# Flame graph of counter-server under 10 clients x 10K requests.
#   scripts/flamegraph.sh memory|disk [oncpu|offcpu]
# oncpu (default) samples the CPU at 999 Hz. offcpu records every context
# switch (the software event, so no tracefs/root needed): each sample is a
# point where a thread went to sleep, so the graph shows where threads block,
# weighted by the number of blockings rather than their duration.
# Samples from all event-loop threads are merged under one NIO-ELT root so the
# graph shows where the event loops spend time, not one tower per thread.
# Writes reports/img/flame-<store>.svg, and a PNG next to it when an SVG
# converter (rsvg-convert or ImageMagick) is available.
#
# Needs perf, stackcollapse-perf.pl and flamegraph.pl (nixpkgs: perf,
# flamegraph). They come from the project's Nix flake; if they are not on
# PATH the script re-runs itself under `direnv exec`.
# perf needs kernel.perf_event_paranoid <= 2 for user-space samples of our own
# process, and <= 1 to see kernel frames (fsync) as well.
set -euo pipefail

store=${1:?usage: scripts/flamegraph.sh memory|disk [oncpu|offcpu]}
mode=${2:-oncpu}
CLIENTS=${CLIENTS:-10}
REQUESTS=${REQUESTS:-10000}

missing_tools() {
    local tool missing=()
    for tool in perf stackcollapse-perf.pl flamegraph.pl; do
        command -v "$tool" >/dev/null || missing+=("$tool")
    done
    printf '%s\n' "${missing[@]}"
}

script_dir=$(cd "$(dirname "$0")" && pwd)
if [ -n "$(missing_tools)" ] && [ -z "${HLS_FLAMEGRAPH_REEXEC:-}" ] && command -v direnv >/dev/null; then
    HLS_FLAMEGRAPH_REEXEC=1 exec direnv exec "$script_dir/.." "$0" "$@"
fi
if [ -n "$(missing_tools)" ]; then
    echo "missing tools: $(missing_tools | tr '\n' ' ')(nixpkgs: perf, flamegraph)" >&2
    exit 1
fi

. "$script_dir/env.sh"

perf_pid=
cleanup() {
    [ -n "$perf_pid" ] && kill -INT "$perf_pid" 2>/dev/null || true
    hls_stop_server
}
trap cleanup EXIT

hls_build_release --product counter-server
hls_build_release --product loadgen
mkdir -p "$HLS_DATA" "$HLS_ROOT/reports/img"

case "$mode" in
    oncpu)  suffix=; event_args=(-F 999); title_kind="on-CPU" ;;
    offcpu) suffix=-offcpu; event_args=(-e context-switches -c 1); title_kind="off-CPU, context switches" ;;
    *) echo "unknown mode: $mode (oncpu|offcpu)" >&2; exit 1 ;;
esac
perf_data="$HLS_DATA/perf-$store$suffix.data"
svg="$HLS_ROOT/reports/img/flame-$store$suffix.svg"
png="${svg%.svg}.png"

hls_start_server "$store"
perf record "${event_args[@]}" -g --call-graph dwarf -p "$HLS_SERVER_PID" -o "$perf_data" 2>"$HLS_DATA/perf-$store$suffix.log" &
perf_pid=$!
sleep 1
"$HLS_BIN/loadgen" --header --clients "$CLIENTS" --requests "$REQUESTS" \
    --url "$SERVER_URL" --store-label "$store"
kill -INT "$perf_pid"
wait "$perf_pid" || true
perf_pid=
hls_stop_server

perf script -i "$perf_data" \
    | swift demangle --simplified \
    | stackcollapse-perf.pl \
    | sed -E 's/^NIO-ELT-[0-9]+-#[0-9]+([; ])/NIO-ELT\1/' \
    | flamegraph.pl --title "counter-server --store $store, $title_kind ($CLIENTS clients x $REQUESTS requests)" \
    >"$svg"
echo "wrote $svg" >&2

if command -v rsvg-convert >/dev/null; then
    rsvg-convert --width 1800 --background-color white "$svg" -o "$png"
elif command -v magick >/dev/null; then
    magick -density 150 "$svg" "$png"
else
    echo "no SVG converter found; SVG only" >&2
    exit 0
fi
echo "wrote $png" >&2

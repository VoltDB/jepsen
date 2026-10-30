#!/usr/bin/env bash
# Runs the LazyFS durability suite (ENG-30396): every power-loss scenario
# against every workload that supports it, one `lein run test` per cell, and
# writes one timestamped results directory holding each cell's Jepsen store,
# a manifest of what was tested and a pass/fail summary.
#
#   resources/lazyfs/run-suite.sh --tarball URL -n HOST -n HOST -n HOST [options] [-- lein args]
#
# See "Running the suite" in README.md, or --help.

set -u -o pipefail

usage() {
    cat << 'USAGE'
Usage: resources/lazyfs/run-suite.sh --tarball URL -n HOST [-n HOST ...] [options] [-- lein args]

Runs every cell of the LazyFS suite, one `lein run test` each, from the
jepsen-volt checkout this script is in.

Required:
  --tarball URL         VoltDB kit, file:// on this control node or http(s)://
  -n, --node HOST       a DB node; repeat it (at least --node-count of them)
  --nodes-file FILE     or read the DB nodes from FILE, one per line

Options:
  -l, --license FILE    VoltDB license on this control node (default: license.xml)
  --scenarios LIST      comma-separated subset of: power-loss, async, torn,
                        snapshot, node (default: all five)
  --workloads LIST      comma-separated subset of: set, rw-register,
                        list-append, bank, redundant-register, single
                        (default: all six; async runs set only)
  --time-limit SECONDS  seconds of load per cell (default: 300)
  --nemesis-interval S  mean seconds between faults (default: 30)
  --node-count N        cluster size, taken from the first N nodes (default: 3)
  --kfactor K           k-factor (default: 1; node needs at least 1)
  --runs N              run every cell N times (default: 1)
  --seed N              seed of the first run; each later run gets the next
                        one (default: random)
  --results DIR         where to write results (default:
                        results/lazyfs-<UTC time> in the checkout)
  --dry-run             print the cells and their commands, run nothing
  -h, --help            this text

Everything after `--` is passed to every `lein run test`, for example
`-- --username ubuntu --ssh-private-key /home/me/.ssh/id_rsa`.

Exit status: 0 when every cell passed, 1 when any did not, 2 on bad usage.
USAGE
}

die_usage() {
    echo "ERROR: $*" >&2
    echo "Run with --help for usage." >&2
    exit 2
}

ALL_SCENARIOS="power-loss async torn snapshot node"
ALL_WORKLOADS="set rw-register list-append bank redundant-register single"

TARBALL=
LICENSE=license.xml
NODES=()
SCENARIOS=$ALL_SCENARIOS
WORKLOADS=$ALL_WORKLOADS
TIME_LIMIT=300
NEMESIS_INTERVAL=30
NODE_COUNT=3
KFACTOR=1
RUNS=1
# $RANDOM is 15 bits; two of them give a seed that is unlikely to repeat.
SEED=$(( (RANDOM << 15) | RANDOM ))
RESULTS=
DRY_RUN=
LEIN_ARGS=()

in_list() {
    local needle=$1; shift
    for x in "$@"; do [ "$x" == "$needle" ] && return 0; done
    return 1
}

is_count() {
    case $1 in ''|*[!0-9]*) return 1 ;; *) [ "$1" -gt 0 ] ;; esac
}

while [ $# -gt 0 ]; do
    case $1 in
        --tarball)          TARBALL=${2:-}; shift 2 ;;
        -l|--license)       LICENSE=${2:-}; shift 2 ;;
        -n|--node)          NODES+=("${2:-}"); shift 2 ;;
        --nodes-file)       [ -r "${2:-}" ] || die_usage "cannot read nodes file '${2:-}'"
                            while read -r h _; do
                                [ -n "$h" ] && [ "${h:0:1}" != "#" ] && NODES+=("$h")
                            done < "$2"
                            shift 2 ;;
        --scenarios)        SCENARIOS=${2//,/ }; shift 2 ;;
        --workloads)        WORKLOADS=${2//,/ }; shift 2 ;;
        --time-limit)       TIME_LIMIT=${2:-}; shift 2 ;;
        --nemesis-interval) NEMESIS_INTERVAL=${2:-}; shift 2 ;;
        --node-count)       NODE_COUNT=${2:-}; shift 2 ;;
        --kfactor)          KFACTOR=${2:-}; shift 2 ;;
        --runs)             RUNS=${2:-}; shift 2 ;;
        --seed)             SEED=${2:-}; shift 2 ;;
        --results)          RESULTS=${2:-}; shift 2 ;;
        --dry-run)          DRY_RUN=1; shift ;;
        -h|--help)          usage; exit 0 ;;
        --)                 shift; LEIN_ARGS=("$@"); break ;;
        *)                  die_usage "unknown option '$1'" ;;
    esac
done

[ -n "$TARBALL" ] || die_usage "--tarball is required"
for s in $SCENARIOS; do
    in_list "$s" $ALL_SCENARIOS || die_usage "unknown scenario '$s'"
done
for w in $WORKLOADS; do
    in_list "$w" $ALL_WORKLOADS || die_usage "unknown workload '$w'"
done
for v in TIME_LIMIT NEMESIS_INTERVAL NODE_COUNT RUNS; do
    is_count "${!v}" || die_usage "$v must be a positive integer, got '${!v}'"
done
case $KFACTOR in ''|*[!0-9]*) die_usage "--kfactor must be an integer, got '$KFACTOR'" ;; esac
case $SEED in ''|*[!0-9]*) die_usage "--seed must be a non-negative integer, got '$SEED'" ;; esac
[ "${#NODES[@]}" -ge "$NODE_COUNT" ] ||
    die_usage "--node-count is $NODE_COUNT but ${#NODES[@]} nodes were given"
if in_list node $SCENARIOS && [ "$KFACTOR" -lt 1 ]; then
    die_usage "the node scenario needs --kfactor 1 or more"
fi

# lein runs from the checkout root, where project.clj and store/ are.
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$ROOT" || exit 2
[ -f project.clj ] || die_usage "$ROOT is not a jepsen-volt checkout"

# The cells, in order: one scenario/workload pair each, with the nemesis and
# the extra options that make up that scenario.
CELLS=()
for s in $SCENARIOS; do
    for w in $WORKLOADS; do
        # the asynchronous command log checker exists for set only
        [ "$s" == async ] && [ "$w" != set ] && continue
        CELLS+=("$s:$w")
    done
done
[ "${#CELLS[@]}" -gt 0 ] || die_usage "no cells: async runs only the set workload"

if [ -z "$DRY_RUN" ]; then
    [ -r "$LICENSE" ] || die_usage "cannot read license '$LICENSE'"
    command -v lein > /dev/null || die_usage "lein is not on PATH"
fi

scenario_args() {
    case $1 in
        power-loss) echo "--nemesis power-loss" ;;
        async)      echo "--nemesis power-loss --commandlog-async" ;;
        torn)       echo "--nemesis torn-power-loss" ;;
        snapshot)   echo "--nemesis snapshot-power-loss" ;;
        node)       echo "--nemesis node-power-loss" ;;
    esac
}

NODE_ARGS=()
for h in "${NODES[@]}"; do NODE_ARGS+=(-n "$h"); done

# Sets CMD to the lein command line of one cell.
cell_command() {
    local scenario=$1 workload=$2 seed=$3
    CMD=(lein run test --tarball "$TARBALL" -l "$LICENSE" -w "$workload"
         $(scenario_args "$scenario") "${NODE_ARGS[@]}"
         --node-count "$NODE_COUNT" --kfactor "$KFACTOR"
         --time-limit "$TIME_LIMIT" --nemesis-interval "$NEMESIS_INTERVAL"
         --seed "$seed" ${LEIN_ARGS[@]+"${LEIN_ARGS[@]}"})
}

if [ -n "$DRY_RUN" ]; then
    seed=$SEED
    for run in $(seq 1 "$RUNS"); do
        for cell in "${CELLS[@]}"; do
            cell_command "${cell%%:*}" "${cell#*:}" "$seed"
            printf '%q ' "${CMD[@]}"; echo
            seed=$(( seed + 1 ))
        done
    done
    exit 0
fi

STARTED=$(date -u +%Y%m%dT%H%M%SZ)
RESULTS=${RESULTS:-results/lazyfs-$STARTED}
mkdir -p "$RESULTS" || exit 2
RESULTS=$(cd "$RESULTS" && pwd)
SUMMARY_TSV=$RESULTS/summary.tsv
MANIFEST=$RESULTS/manifest.txt

# The LazyFS version and patches the harness builds, as pinned in lazyfs.clj.
lazyfs_field() {
    grep -m1 "^ *{\{0,1\} *$1 " src/jepsen/voltdb/lazyfs.clj | sed 's/^[^"]*//; s/[]})]*$//; s/"//g'
}

sha256() {
    { sha256sum "$1" 2>/dev/null || shasum -a 256 "$1"; } | cut -d' ' -f1
}

# The kit's checksum identifies the build exactly; volt.log only has its version.
case $TARBALL in
    file://*) KIT_SUM="sha256 $(sha256 "${TARBALL#file://}")" ;;
    *)        KIT_SUM="(not a local file)" ;;
esac

{
    echo "suite              LazyFS durability suite (resources/lazyfs/run-suite.sh)"
    echo "started            $STARTED"
    echo "control node       $(hostname)"
    echo "jepsen-volt        $(git rev-parse HEAD 2>/dev/null)$(git diff --quiet HEAD 2>/dev/null || echo ' (with local changes)')"
    echo "lazyfs             $(lazyfs_field :tag) commit $(lazyfs_field :commit) patches $(lazyfs_field :patches)"
    for p in resources/lazyfs/*.patch; do
        echo "lazyfs patch       $p sha256 $(sha256 "$p")"
    done
    echo "voltdb tarball     $TARBALL"
    echo "voltdb checksum    $KIT_SUM"
    echo "voltdb build       (filled in after the first cell)"
    echo "license            $LICENSE"
    echo "java (control)     $("${JAVA_HOME:+$JAVA_HOME/bin/}java" -version 2>&1 | head -1)"
    echo "java (DB nodes)    ${VOLT_JAVA_HOME:-same as the control node}"
    echo "lein               $(lein version 2>/dev/null | head -1)"
    echo "nodes              ${NODES[*]:0:$NODE_COUNT}"
    echo "cluster            $NODE_COUNT nodes, k=$KFACTOR"
    echo "time limit         $TIME_LIMIT s per cell"
    echo "nemesis interval   $NEMESIS_INTERVAL s"
    echo "runs               $RUNS"
    echo "first seed         $SEED"
    echo "scenarios          $SCENARIOS"
    echo "workloads          $WORKLOADS"
    echo "extra lein args    ${LEIN_ARGS[*]:-}"
} > "$MANIFEST"
printf 'cell\tscenario\tworkload\trun\tseed\tresult\texit\tseconds\n' > "$SUMMARY_TSV"

# Classifies a finished cell from lein's exit status and its store:
# PASS, FAIL (a checker found a problem), UNKNOWN (a checker crashed: jepsen
# exits 2), FATAL (a VoltDB FATAL, which the lab job also counts as a
# failure) or ERROR (no results: setup or the harness failed).
classify() {
    local status=$1 dir=$2
    if [ ! -f "$dir/store/results.edn" ]; then
        echo ERROR
    elif grep -qsE ':[0-9]+,[0-9]+[[:space:]]+FATAL' "$dir"/store/*/log/volt.log; then
        echo FATAL
    else
        case $status in
            0) echo PASS ;;
            1) echo FAIL ;;
            2) echo UNKNOWN ;;
            *) echo ERROR ;;
        esac
    fi
}

echo "LazyFS suite: ${#CELLS[@]} cells x $RUNS runs, results in $RESULTS"
total=$(( ${#CELLS[@]} * RUNS ))
n=0
failed=0
seed=$SEED
for run in $(seq 1 "$RUNS"); do
    for cell in "${CELLS[@]}"; do
        n=$(( n + 1 ))
        scenario=${cell%%:*}
        workload=${cell#*:}
        name=$(printf '%02d-%s-%s' "$n" "$scenario" "$workload")
        [ "$RUNS" -gt 1 ] && name=$name-run$run
        dir=$RESULTS/$name
        mkdir -p "$dir"
        cell_command "$scenario" "$workload" "$seed"
        printf '%q ' "${CMD[@]}" > "$dir/command.txt"; echo >> "$dir/command.txt"
        echo "[$n/$total] $name (seed $seed)"

        before=$(readlink store/current 2>/dev/null)
        SECONDS=0
        "${CMD[@]}" > "$dir/jepsen.log" 2> "$dir/stderr.log"
        status=$?
        took=$SECONDS

        # store/current points at this cell's store only if jepsen got as far
        # as creating one
        after=$(readlink store/current 2>/dev/null)
        if [ -n "$after" ] && [ "$after" != "$before" ]; then
            cp -R "$(cd store && cd "$after" && pwd)" "$dir/store"
        fi
        result=$(classify "$status" "$dir")
        echo "$result" > "$dir/result"
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$name" "$scenario" "$workload" \
            "$run" "$seed" "$result" "$status" "$took" >> "$SUMMARY_TSV"
        echo "[$n/$total] $name: $result in ${took}s"
        [ "$result" == PASS ] || failed=$(( failed + 1 ))

        if grep -q '^voltdb build *(filled' "$MANIFEST"; then
            build=$(grep -h -m1 -o 'Build: .*' "$dir"/store/*/log/volt.log 2>/dev/null | head -1)
            build=${build#Build: }
            [ -n "$build" ] && sed -i.bak "s|^voltdb build .*|voltdb build       ${build//|/\\|}|" "$MANIFEST" &&
                rm -f "$MANIFEST.bak"
        fi
        seed=$(( seed + 1 ))
    done
done

passed=$(( total - failed ))
{
    echo "LazyFS suite $STARTED: $passed of $total cells passed"
    echo
    column -t -s $'\t' "$SUMMARY_TSV" 2>/dev/null || cat "$SUMMARY_TSV"
    echo
    [ "$failed" -eq 0 ] && echo "PASS" || echo "FAIL"
} > "$RESULTS/summary.txt"
cat "$RESULTS/summary.txt"
echo "Results: $RESULTS"
[ "$failed" -eq 0 ]

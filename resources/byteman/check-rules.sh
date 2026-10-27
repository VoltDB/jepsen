#!/bin/bash
# Type-checks Byteman rule files against a VoltDB build's classes, so a
# misspelled class, method or field fails here instead of as a rule that
# silently never fires on a cluster.
#
#   resources/byteman/check-rules.sh <voltdb> [rule.btm ...]
#
# <voltdb> is an unpacked kit (voltdb-ent-<version>/) or an internal checkout
# after a build; either way it has voltdb/*.jar and lib/*.jar. With no rule
# files, checks every *.btm next to this script. Needs Java 17+ on the PATH
# or in JAVA_HOME, and the Byteman jar in ~/.m2 (any lein command in this
# repo fetches it). Exits non-zero if any rule has an error.
set -euo pipefail

usage() { sed -n '5,13p' "$0" | sed 's/^# \{0,1\}//' >&2; exit 2; }
[ $# -ge 1 ] || usage
VOLTDB=$1; shift

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
VERSION=$(grep -o 'org.jboss.byteman/byteman "[^"]*"' "$ROOT/project.clj" | grep -o '"[^"]*"' | tr -d '"')
BYTEMAN=~/.m2/repository/org/jboss/byteman/byteman/$VERSION/byteman-$VERSION.jar
[ -f "$BYTEMAN" ] || { echo "No $BYTEMAN; run 'lein deps' in $ROOT first" >&2; exit 1; }

shopt -s nullglob
JARS=("$VOLTDB"/voltdb/*.jar "$VOLTDB"/lib/*.jar)
[ ${#JARS[@]} -gt 0 ] || { echo "No voltdb/*.jar or lib/*.jar under $VOLTDB" >&2; exit 1; }
CP=$(IFS=:; echo "${JARS[*]}")

[ $# -gt 0 ] || set -- "$HERE"/*.btm
JAVA=${JAVA_HOME:+$JAVA_HOME/bin/}java

# TestScript prints one block per rule; keep the verdict lines.
OUT=$("$JAVA" -cp "$BYTEMAN:$CP" org.jboss.byteman.check.TestScript -v "$@" 2>&1) || true
echo "$OUT" | grep -E "^(Checking rule|Type checked|ERROR|WARNING|TestScript:|  *[0-9]+ total)" || echo "$OUT"
echo "$OUT" | grep -q "TestScript: no errors"

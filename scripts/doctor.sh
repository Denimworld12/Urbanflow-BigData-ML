#!/usr/bin/env bash
# Run this first, on every machine. It catches the problems that eat week 1.
set -u
ok(){ printf "  \033[32mOK\033[0m   %s\n" "$1"; }
bad(){ printf "  \033[31mFAIL\033[0m %s\n" "$1"; }
warn(){ printf "  \033[33mWARN\033[0m %s\n" "$1"; }

echo "UrbanFlow environment check"
echo

# parse from the whole output: JAVA_TOOL_OPTIONS and agent banners can precede the version line
V=$(java -version 2>&1 | grep -iE '(openjdk|java) version' | head -1 | grep -oE '"[0-9]+' | tr -d '"')
if [ -z "${V:-}" ]; then bad "Java not found. Install JDK 17, 21 or 25 (README, Native setup)."
elif [ "$V" -ge 17 ] 2>/dev/null; then ok "Java $V"
else bad "Java $V is too old. Spark 4.2 needs 17, 21 or 25. Install Temurin 17."; fi

# same interpreter `make setup` uses; check another with PYTHON=python3.12 scripts/doctor.sh
PY=${PYTHON:-python3}
P=$($PY -c 'import sys;print("%d.%d"%sys.version_info[:2])' 2>/dev/null)
if [ -z "$P" ]; then bad "$PY not found"
elif $PY -c 'import sys; sys.exit(sys.version_info < (3, 10))'; then ok "Python $P ($PY)"
else bad "$PY is Python $P; pyspark 4.2 needs 3.10+. Install a newer one, then make setup PYTHON=python3.12"; fi

C=$( (nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null) || echo "?")
ok "CPU cores: $C"

if command -v free >/dev/null; then
  G=$(free -g | awk '/^Mem:/{print $2}')
  [ "$G" -ge 7 ] 2>/dev/null && ok "RAM ${G} GB" || warn "RAM ${G} GB — use TIER=0/1 only, set URBANFLOW_DRIVER_MEM=3g"
fi

A=$(df -Pk . | awk 'NR==2{printf "%d", $4/1048576}')
[ "$A" -ge 25 ] 2>/dev/null && ok "Free disk ${A} GB" || warn "Free disk ${A} GB — need 25 GB for Tier 2"

echo
echo "If everything above is OK:  make setup${PYTHON:+ PYTHON=$PYTHON} && make check && make synth"

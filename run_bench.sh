#!/usr/bin/env bash
#
# run_bench.sh -- micro-benchmark runner for the Scala binding.
# Builds the Java binding (libitb3.so + JNI shim + jars) and the sbt
# projects via build.sh, points ITB_JNI_PATH at the freshly-built JNI
# shim, then runs the bench main: encryptMessage and stream-pump
# throughput at 1 MiB / 16 MiB / 64 MiB.
#
# build.sh wipes every sbt target directory and the eitb classpath
# cache, and delegates the Java layer to bindings/java/build.sh, which
# cleans its own, so the classes measured here are always compiled by
# this invocation. Set ITB_SKIP_CLEAN=1 to keep the existing artefacts
# and compile incrementally instead.
#
# Usage:
#   ./run_bench.sh             # both shapes
#   ./run_bench.sh message     # Single Message shape only
#   ./run_bench.sh stream      # stream-pump shape only

set -eu
set -o pipefail

cd "$(dirname "$0")"
REPO_ROOT="$(cd ../.. && pwd)"

./build.sh

export ITB_JNI_PATH="$REPO_ROOT/bindings/java/build/jni/libitb3_jni.so"

# Go-runtime pacing defaults for bench-scale allocation churn; the
# `:-` form respects any override set by the caller. The bench main
# applies the same caps programmatically.
export ITB_GOMEMLIMIT="${ITB_GOMEMLIMIT:-4GiB}"
export ITB_GOGC="${ITB_GOGC:-100}"

# Bench-shape defaults — match the root Go BENCH3.md pin so the
# throughput numbers are directly comparable to the shipped Go
# Encrypt3x{128,256,512}Cfg baseline. Override any of these before
# calling the script to change the shape.
export ITB_NONCE_BITS="${ITB_NONCE_BITS:-512}"
export ITB_KEY_BITS="${ITB_KEY_BITS:-1024}"
export ITB_WITH_PARALLAX="${ITB_WITH_PARALLAX:-false}"
export ITB_WITH_WRAPPER="${ITB_WITH_WRAPPER:-false}"
export ITB_INNER_HASH="${ITB_INNER_HASH:-areion512}"
export ITB_BENCH_MIN_SEC="${ITB_BENCH_MIN_SEC:-5}"

# ITB_WITH_MAC=true derives MAC/AEAD profile counterparts. When
# ITB_PROFILE is set explicitly by the caller, it wins over the
# derivation and applies to both shapes (expert override).
: "${ITB_WITH_MAC:=false}"
if [ -n "${ITB_PROFILE:-}" ]; then
    ITB_MSG_PROFILE_DEFAULT="${ITB_PROFILE}"
    ITB_STREAM_PROFILE_DEFAULT="${ITB_PROFILE}"
elif [ "${ITB_WITH_MAC}" = "true" ]; then
    ITB_MSG_PROFILE_DEFAULT="singlemsg-triple-mac-v1"
    ITB_STREAM_PROFILE_DEFAULT="streaming-aead-triple-mac-v1"
else
    ITB_MSG_PROFILE_DEFAULT="singlemsg-triple-nomac-v1"
    ITB_STREAM_PROFILE_DEFAULT="streaming-noaead-triple-v1"
fi

# Split at the shell layer so each shape carries its own ITB_PROFILE
# in a single script pass (the bench entry point handles "message",
# "stream", and "all" arguments individually).
case "${1:-all}" in
    message)
        export ITB_PROFILE="${ITB_MSG_PROFILE_DEFAULT}"
        exec sbt --batch --error "bench/run message"
        ;;
    stream)
        export ITB_PROFILE="${ITB_STREAM_PROFILE_DEFAULT}"
        exec sbt --batch --error "bench/run stream"
        ;;
    stream_one_shot)
        export ITB_PROFILE="${ITB_STREAM_PROFILE_DEFAULT}"
        exec sbt --batch --error "bench/run stream_one_shot"
        ;;
    all)
        export ITB_PROFILE="${ITB_MSG_PROFILE_DEFAULT}"
        sbt --batch --error "bench/run message"
        export ITB_PROFILE="${ITB_STREAM_PROFILE_DEFAULT}"
        sbt --batch --error "bench/run stream"
        exec sbt --batch --error "bench/run stream_one_shot"
        ;;
    *)
        echo "usage: $0 [message|stream|stream_one_shot|all]" >&2
        exit 2
        ;;
esac

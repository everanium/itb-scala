#!/usr/bin/env bash
#
# One-step build for the Scala binding. The binding is a thin proxy over
# the Java binding (JVM bytecode interop, no FFI hop of its own), so the
# Java binding is built first (libitb3.so + JNI shim + jars via
# bindings/java/build.sh), then sbt compiles the Scala library, tests,
# bench, and eitb. Prerequisites (Go, JDK 17+, Gradle, gcc, sbt) must be
# installed separately; see README.md "Prerequisites" section.
#
# The build starts from an empty tree: every sbt target directory
# (root, bench, eitb and the meta-build under project/), the BSP and
# Bloop tooling caches and the eitb launcher's cached classpath are
# removed first, so no output can survive from an earlier invocation.
#
# eitb/.classpath is the artefact that matters most here. The launcher
# execs a plain JVM against whatever that file holds and only
# regenerates it when it is missing, so a cached classpath naming a jar
# from another version keeps resolving that jar for every later
# invocation. Wiping it on each build, and regenerating it below
# against the freshly-compiled tree, is what keeps the launcher and the
# sbt projects on the same library.
#
# The Java binding layer is a shared producer, not something this
# binding owns, so it is never deleted from here: bindings/java/build.sh
# is invoked instead and performs its own clean.
#
# Set ITB_SKIP_CLEAN=1 to keep the existing artefacts and build
# incrementally; it propagates to the Java layer as well. With no
# environment set the wipe always runs.
#
# Usage:
#   ./build.sh             # default build (full asm stack)
#   ./build.sh --noitbasm  # opt out of ITB's SIMD asm kernels

set -eu
set -o pipefail

cd "$(dirname "$0")"
BINDING_DIR="$(pwd -P)"
REPO_ROOT="$(cd ../.. && pwd -P)"
START_EPOCH="$(date +%s)"
SKIP_CLEAN="${ITB_SKIP_CLEAN:-0}"

TAGS=()
case "${1:-}" in
    --noitbasm) TAGS=(-tags=noitbasm); shift;;
    -h|--help)  echo "usage: $0 [--noitbasm]"; exit 0;;
    "")         ;;
    *)          echo "unknown option: $1" >&2; exit 2;;
esac

# clean_under <root> <relative-path>...
#
# Removes each relative path under <root>. A target is removed only
# when it is a literal relative path (no leading slash, no ".."), it
# exists, and it still resolves inside <root> after symlinks are
# followed -- so a target can never escape the tree it belongs to.
# Every removal is logged before it happens, and a failing rm aborts
# the script rather than being swallowed.
clean_under() {
    local root="$1"; shift
    local rel abs
    root="$(realpath -e "$root")"
    for rel in "$@"; do
        case "$rel" in
            "" | /* | *..*)
                echo "clean: refusing suspicious target '$rel'" >&2
                exit 1
                ;;
        esac
        abs="$root/$rel"
        if [ ! -e "$abs" ] && [ ! -L "$abs" ]; then
            echo "[clean] (absent) $abs"
            continue
        fi
        abs="$(realpath -e "$abs")"
        case "$abs/" in
            "$root"/?*) ;;
            *)
                echo "clean: refusing to remove '$abs' -- outside $root" >&2
                exit 1
                ;;
        esac
        echo "[clean] rm -rf $abs"
        rm -rf "$abs"
    done
}

# require_built <path>
#
# Asserts that a build artefact exists and, when the clean stage ran,
# that it was written by this invocation rather than inherited from an
# earlier one.
require_built() {
    local f="$1"
    if [ ! -f "$f" ]; then
        echo "build.sh: expected artefact was not produced: $f" >&2
        exit 1
    fi
    if [ "$SKIP_CLEAN" != "1" ] && [ "$(stat -c %Y "$f")" -lt "$START_EPOCH" ]; then
        echo "build.sh: artefact predates this invocation: $f" >&2
        exit 1
    fi
}

if [ "$SKIP_CLEAN" = "1" ]; then
    echo "==> ITB_SKIP_CLEAN=1 — keeping existing artefacts"
else
    echo "==> cleaning Scala binding artefacts"
    clean_under "$BINDING_DIR" \
        target bench/target eitb/target project/target project/project \
        .bsp .bloop .metals eitb/.classpath
fi

echo "==> building Java binding (libitb3.so + JNI shim + jars)"
bash "$REPO_ROOT/bindings/java/build.sh"

# Re-run the libitb3 build when a tag opt-out is requested — the Java
# driver builds the default (full asm) shared library; the JNI shim
# resolves dist/linux-amd64/libitb3.so by RPATH, so overwriting the
# file in place retargets it.
if [ "${#TAGS[@]}" -gt 0 ]; then
    cd "$REPO_ROOT"
    echo "==> rebuilding libitb3.so (with ${TAGS[*]})"
    go build -trimpath "${TAGS[@]}" -buildmode=c-shared \
        -o dist/linux-amd64/libitb3.so ./cmd/cshared
fi

cd "$BINDING_DIR"
echo "==> building Scala binding (sbt compile)"
sbt --batch compile Test/compile bench/Compile/compile eitb/Compile/compile loop/Compile/compile

echo "==> refreshing eitb launcher classpath cache"
sbt --batch --error "export eitb/Runtime/fullClasspath" | tail -n 1 > eitb/.classpath

# `export` prints the classpath on stdout, but any sbt diagnostic that
# reaches stdout would be the line `tail` captures instead. Reject a
# cache that does not look like a classpath carrying the Java binding
# jar, so the launcher never execs a JVM against a truncated or
# diagnostic-filled entry.
if ! grep -q 'libitb3-java-' eitb/.classpath; then
    echo "build.sh: eitb/.classpath does not name the Java binding jar:" >&2
    cat eitb/.classpath >&2
    exit 1
fi
require_built "$BINDING_DIR/eitb/.classpath"

echo "==> refreshing loop launcher classpath cache"
sbt --batch --error "export loop/Runtime/fullClasspath" | tail -n 1 > loop/.classpath
if ! grep -q 'libitb3-java-' loop/.classpath; then
    echo "build.sh: loop/.classpath does not name the Java binding jar:" >&2
    cat loop/.classpath >&2
    exit 1
fi
require_built "$BINDING_DIR/loop/.classpath"

echo "==> ready: ./run_tests.sh"

#!/usr/bin/env bash
#
# Fleet entry point for the loop stress harness of the Scala binding:
# compiles the utility on first use and caches its runtime classpath in
# loop/.classpath, then execs a plain JVM — so repeat invocations skip
# sbt start-up entirely. libitb3.so, the JNI shim and the Java binding
# layer are assumed built by build.sh, which also refreshes the cache.
# Delete loop/.classpath (or re-run build.sh) after source changes.
#
# Usage:
#   ./run_loop.sh --duration 2m --shape both

set -eu
set -o pipefail

cd "$(dirname "$0")"
REPO_ROOT="$(cd ../.. && pwd)"

if [ ! -s loop/.classpath ]; then
    echo "==> first use: compiling loop + caching classpath" >&2
    sbt --batch loop/compile >&2
    sbt --batch --error "export loop/Runtime/fullClasspath" | tail -n 1 > loop/.classpath
fi

export ITB_JNI_PATH="${ITB_JNI_PATH:-$REPO_ROOT/bindings/java/build/jni/libitb3_jni.so}"

exec java -cp "$(cat loop/.classpath)" io.github.everanium.itb3.scala.loop.Main "$@"

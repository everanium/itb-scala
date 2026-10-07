#!/usr/bin/env bash
#
# One-step test runner for the Scala binding. Builds the Java binding
# (libitb3.so + JNI shim + jars) and the sbt projects via build.sh,
# points ITB_JNI_PATH at the freshly-built JNI shim, then invokes `sbt
# test`. A positional argument narrows the run to a testOnly glob.
#
# build.sh wipes every sbt target directory and the eitb classpath
# cache, and delegates the Java layer to bindings/java/build.sh, which
# cleans its own, so the classes exercised here are always compiled by
# this invocation. Set ITB_SKIP_CLEAN=1 to keep the existing artefacts
# and compile incrementally instead.
#
# Usage:
#   ./run_tests.sh                        # all suites
#   ./run_tests.sh '*SmokeSuite'          # testOnly glob

set -eu
set -o pipefail

cd "$(dirname "$0")"
REPO_ROOT="$(cd ../.. && pwd)"

./build.sh

export ITB_JNI_PATH="$REPO_ROOT/bindings/java/build/jni/libitb3_jni.so"

if [ "$#" -gt 0 ]; then
    exec sbt --batch "testOnly $*"
fi
exec sbt --batch test

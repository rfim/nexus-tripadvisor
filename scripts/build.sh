#!/usr/bin/env bash
# Rebuild everything the site shows from a clean run: simulate the topics, run the jobs, the batch, the checks, export.
set -euo pipefail
cd "$(dirname "$0")/.."
rm -rf target landing
export PYTHONPATH=src TZ=UTC
if command -v mvn >/dev/null; then   # the Flink job, so the run can check it against the Python job
  mvn -q -B -f flink/job/pom.xml compile dependency:build-classpath -Dmdep.outputFile=target/cp.txt
fi
python -m nexus.pipeline

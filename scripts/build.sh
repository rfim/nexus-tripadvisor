#!/usr/bin/env bash
# Rebuild everything the site shows from a clean run: simulate the topics, run the jobs, the batch, the checks, export.
# The console output is kept in assets/data/build_log.txt: the streaming page shows it as "the server, as it ran".
set -euo pipefail
cd "$(dirname "$0")/.."
rm -rf target landing
mkdir -p target
export PYTHONPATH=src TZ=UTC
LOG=target/build_log.txt
{
  echo "\$ ./scripts/build.sh            # $(date -u +%Y-%m-%dT%H:%M:%SZ) on $(uname -sm)"
  echo "\$ python --version && java -version 2>&1 | head -1 && mvn -v | head -1"
  python --version; java -version 2>&1 | sed '/JAVA_TOOL/d' | head -1; (mvn -v 2>/dev/null | head -1) || echo "maven: not installed"
} > "$LOG"
if command -v mvn >/dev/null; then   # the Flink job, so the run can check it against the Python job
  echo "\$ mvn -q -B -f flink/job/pom.xml compile dependency:build-classpath" >> "$LOG"
  mvn -q -B -f flink/job/pom.xml compile dependency:build-classpath -Dmdep.outputFile=target/cp.txt 2>&1 | sed '/JAVA_TOOL/d' | tee -a "$LOG"
  echo "[INFO] flink/job compiled: $(ls flink/job/target/classes/nexus/flink | wc -l) classes" >> "$LOG"
fi
echo "\$ python -m nexus.pipeline" >> "$LOG"
# Spark's start-up chatter and progress bars are left out of the kept log
python -m nexus.pipeline 2>&1 | sed '/JAVA_TOOL/d; / WARN /d; /\[Stage /d; /default log level/d; /setLogLevel/d' | tee -a "$LOG"
cp "$LOG" assets/data/build_log.txt

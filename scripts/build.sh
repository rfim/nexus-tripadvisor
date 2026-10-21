#!/usr/bin/env bash
# Rebuild everything the site shows from a clean run: simulate the topics, run the jobs, the batch, the checks, export.
set -euo pipefail
cd "$(dirname "$0")/.."
rm -rf target landing
export PYTHONPATH=src TZ=UTC
python -m nexus.pipeline

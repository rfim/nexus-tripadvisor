"""Where things live. No third-party imports, so every module can use it, in CI and in the browser."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LANDING = Path(os.environ.get("NEXUS_LANDING", ROOT / "landing"))   # the simulated Kafka logs
TARGET = Path(os.environ.get("NEXUS_TARGET", ROOT / "target"))      # everything a run writes

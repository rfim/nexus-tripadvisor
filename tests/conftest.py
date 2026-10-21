import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
os.environ["TZ"] = "UTC"
if hasattr(time, "tzset"):
    time.tzset()

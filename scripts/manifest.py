"""List the files the site's code editor can open: assets/data/files.json. Run by CI."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GLOBS = ["examples/*.py", "src/nexus/*.py", "java/src/nexus/*.java", "flink/*.sql", "contracts/*.yml", "tests/*.py",
         "warehouse/snowflake/*.sql", "warehouse/bigquery/*.sql", "scripts/*.sh", "scripts/*.py", "docs/pdf-src/build.py",
         ".github/workflows/ci.yml", "requirements.txt"]
SPARK = {"spark_batch.py", "test_spark_batch.py"}
JAVA = {"test_java_twin.py"}


def kind(rel: str, name: str) -> str:
    if rel.startswith("examples/") or (rel.startswith("src/") and name not in SPARK):
        return "browser"
    if name in SPARK:
        return "spark"
    if rel.startswith("java/") or name in JAVA:
        return "java"
    if rel.startswith(("flink/", "warehouse/")):
        return "local"
    return "tests" if rel.startswith("tests/") else "view"


def manifest() -> list[dict]:
    out = []
    for g in GLOBS:
        for p in sorted(ROOT.glob(g)):
            rel = p.relative_to(ROOT).as_posix()
            out.append({"path": rel, "folder": rel.rsplit("/", 1)[0] if "/" in rel else ".", "runs": kind(rel, p.name),
                        "lines": len(p.read_text().splitlines())})
    return out


if __name__ == "__main__":
    (ROOT / "assets" / "data" / "files.json").write_text(json.dumps(manifest(), indent=1) + "\n")
    print(len(manifest()), "files")

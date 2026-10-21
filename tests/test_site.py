"""The pages: every element a script fills exists, links point at real files, and the strip has a pill per section."""
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HTML = (ROOT / "index.html").read_text()
PAGES = {"index.html": "home.js", "stream.html": "stream.js", "governance.html": "governance.js", "oncall.html": "oncall.js", "run.html": "code.js"}


def test_every_id_each_script_fills_exists():
    for page, js in PAGES.items():
        html, code = (ROOT / page).read_text(), (ROOT / "assets" / js).read_text()
        for i in re.findall(r'\$\("([\w-]+)"\)|getElementById\("([\w-]+)"\)', code):
            i = i[0] or i[1]
            assert f'id="{i}"' in html or f'id="{i}"' in code, (page, i)   # in the page, or drawn by the script


def test_pages_link_to_each_other():
    for page in PAGES:
        html = (ROOT / page).read_text()
        assert all(f'href="{other}"' in html for other in PAGES if other != page), page


def test_repository_links_point_at_real_paths():
    for page in [*PAGES, "story.html"]:
        for path in re.findall(r"github\.com/rfim/nexus-tripadvisor/(?:tree|blob)/main/([\w./-]+)", (ROOT / page).read_text()):
            assert (ROOT / path).exists(), (page, path)


def test_local_assets_exist():
    for page in PAGES:
        for ref in re.findall(r'(?:src|href)="((?:assets|flink|warehouse)/[^"#?]+)', (ROOT / page).read_text()):
            assert (ROOT / ref).exists(), (page, ref)


def test_strip_has_one_pill_per_section():
    strip = HTML.split('id="strip"')[1]
    assert len(re.findall(r'data-go="\d"', HTML)) == strip.count("<section ")


def test_stated_plainly():
    assert "not affiliated with or endorsed by Tripadvisor" in HTML
    assert "not Flink" in HTML and "Java is the part of the stack I'd be ramping up on" in HTML


def test_the_editor_lists_every_runnable_file_and_they_exist():
    files = json.loads((ROOT / "assets/data/files.json").read_text())
    assert all((ROOT / f["path"]).exists() for f in files)
    runnable = {f["path"] for f in files if f["runs"] == "browser"}
    assert {"examples/03_watermarks_by_hand.py", "examples/04_the_whole_run.py", "src/nexus/stream.py"} <= runnable
    assert not any("pyspark" in (ROOT / p).read_text() for p in runnable)

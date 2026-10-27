"""
Parity between the checked-in DE runbook (sql/) and the Python templates that produce it.

Every file under sql/ is a generated copy of a module's rendered SQL. The DE team runs
the copy; the Python pipeline runs the template. If someone fixes one without the other,
the two paths silently diverge and the next regeneration overwrites the fix. This test
renders each module exactly as sql/README.md documents (``python -m <module> --dry-run``
with the dev project set) and requires the result to equal the file's body.

Only the hand-written metadata banner and trailing whitespace at end of file are
ignored; every SQL line must match. On failure, fix the template under src/ — not the
file under sql/ — then regenerate.

sql/scoring/ is excluded: those files are rendered by scripts/generate_scoring_sql.py
from fitted weights read out of BigQuery, so they cannot be rendered offline.
"""
import difflib
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = REPO_ROOT / "sql"
DEV_PROJECT = "clgx-gis-app-dev-06e3"

_BANNER_RULE = re.compile(r"^-- =+$")
_MODULE_LINE = re.compile(r"^-- Module *: *(\S+)", re.MULTILINE)

RUNBOOK_FILES = sorted(
    p for p in SQL_DIR.rglob("*.sql") if p.relative_to(SQL_DIR).parts[0] != "scoring"
)


def _strip_banner(text: str) -> str:
    """Drop the leading `-- ====` banner block and the blank line that follows it."""
    lines = text.splitlines(keepends=True)
    rules = [i for i, line in enumerate(lines) if _BANNER_RULE.match(line.rstrip("\n"))]
    body = lines[rules[1] + 1:]
    if body and not body[0].strip():
        body = body[1:]
    return "".join(body)


def _render(module: str) -> str:
    env = dict(os.environ, GCS_PROJECT_ID=DEV_PROJECT)
    proc = subprocess.run(
        [sys.executable, "-m", module, "--dry-run"],
        capture_output=True, text=True, env=env, cwd=REPO_ROOT, timeout=120,
    )
    assert proc.returncode == 0, f"{module} --dry-run failed:\n{proc.stderr}"
    return proc.stdout


def test_runbook_files_were_discovered():
    assert len(RUNBOOK_FILES) >= 16


@pytest.mark.parametrize(
    "sql_file", RUNBOOK_FILES, ids=lambda p: str(p.relative_to(SQL_DIR))
)
def test_runbook_file_matches_its_template(sql_file):
    text = sql_file.read_text()
    match = _MODULE_LINE.search(text)
    assert match, f"{sql_file} has no '-- Module :' header naming its source module"

    expected = _strip_banner(text).rstrip()
    actual = _render(match.group(1)).rstrip()

    if actual != expected:
        diff = "\n".join(difflib.unified_diff(
            expected.splitlines(), actual.splitlines(),
            fromfile=f"sql/{sql_file.relative_to(SQL_DIR)}",
            tofile=f"{match.group(1)} --dry-run", lineterm="",
        ))
        pytest.fail(
            "Runbook file and its template have diverged. Fix the template under src/, "
            f"then regenerate — never hand-edit sql/.\n{diff}"
        )

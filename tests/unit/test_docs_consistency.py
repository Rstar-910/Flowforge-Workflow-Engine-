"""The failure model and correctness audit only cite tests that actually exist."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
DOCS = ["docs/failure-model.md", "docs/correctness-audit.md", "docs/state-machines.md"]
CITED = re.compile(r"`(test_[A-Za-z0-9_]+)(…|\.\.\.)?(?:\[[^\]]*\])?`")


def defined_tests() -> set[str]:
    names: set[str] = set()
    for path in (ROOT / "tests").rglob("test_*.py"):
        names.update(re.findall(r"^(?:async )?def (test_\w+)", path.read_text(), re.M))
    return names


@pytest.mark.parametrize("doc", DOCS)
def test_every_cited_test_exists(doc: str) -> None:
    text = (ROOT / doc).read_text()
    cited = CITED.findall(text)
    assert cited or doc.endswith("state-machines.md"), f"{doc} cites no tests"
    tests = defined_tests()
    missing = []
    for name, ellipsis in cited:
        found = any(t.startswith(name) for t in tests) if ellipsis else name in tests
        if not found:
            missing.append(name + ellipsis)
    assert not missing, f"{doc} cites tests that do not exist: {missing}"


def test_failure_model_rows_all_cite_a_test_or_say_why_not() -> None:
    """Every scenario row in the failure-model table names its evidence."""
    text = (ROOT / "docs/failure-model.md").read_text()
    table = text[text.index("| scenario | behaviour | test |") :]
    rows = [r for r in table.splitlines()[2:] if r.startswith("|")]
    assert len(rows) >= 20
    for row in rows:
        evidence = row.rstrip("|").rsplit("|", 1)[-1]
        assert CITED.search(evidence) or "by construction" in evidence or "see [" in evidence, row

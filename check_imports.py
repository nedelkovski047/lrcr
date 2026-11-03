"""
Compare what your code imports with what requirements.txt installs.

    python check_imports.py            (run in the venv you develop in, from the project folder)

Lists every third-party import in the *.py files here that requirements.txt does not cover,
with the package name and the version installed in this venv, ready to paste into
requirements.txt. Exit code 1 when something is missing, so it can gate a deploy.
"""

from __future__ import annotations

import ast
import re
import sys
from importlib import metadata
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SKIP_DIRS = {"venv", ".venv", "env", "__pycache__", "node_modules", ".git", "tests"}


def normalize(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def project_files() -> list[Path]:
    return [p for p in ROOT.rglob("*.py") if not SKIP_DIRS.intersection(p.relative_to(ROOT).parts)]


def imported_top_levels(files: list[Path]) -> set[str]:
    found: set[str] = set()
    for path in files:
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError) as exc:
            print(f"  ! could not parse {path.name}: {exc}")
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                found.update(a.name.split(".")[0] for a in node.names)
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                found.add(node.module.split(".")[0])
    return found


def required_distributions() -> set[str]:
    names: set[str] = set()
    for line in (ROOT / "requirements.txt").read_text(encoding="utf-8").splitlines():
        line = line.split("#", 1)[0].strip()
        if line and not line.startswith("-"):
            names.add(normalize(re.split(r"[\[<>=!~ ;]", line, maxsplit=1)[0]))
    return names


def main() -> int:
    local = {p.stem for p in project_files()}
    stdlib = set(sys.stdlib_module_names)
    third_party = {m for m in imported_top_levels(project_files()) if m not in stdlib and m not in local}
    module_to_dists = metadata.packages_distributions()
    required = required_distributions()

    missing: list[str] = []
    unknown: list[str] = []
    for module in sorted(third_party):
        dists = {normalize(d) for d in module_to_dists.get(module, [])}
        if not dists:
            unknown.append(module)
        elif not dists & required:
            best = sorted(dists)[0]
            missing.append(f"{best}=={metadata.version(best)}    # imported as '{module}'")

    if missing:
        print("Add to requirements.txt:")
        print("\n".join("  " + m for m in missing))
    if unknown:
        print("Imported but not installed in this venv (typo, or install it first): " + ", ".join(unknown))
    if not missing and not unknown:
        print(f"OK: all {len(third_party)} third-party imports are covered by requirements.txt")
    return 1 if (missing or unknown) else 0


if __name__ == "__main__":
    raise SystemExit(main())

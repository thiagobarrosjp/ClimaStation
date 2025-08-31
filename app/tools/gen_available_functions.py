#!/usr/bin/env python3
"""
Generate `context/available_functions.md` from public functions and classes.

- Scans under `app/` (or custom globs via --paths)
- Uses the first line of the docstring as the summary; falls back to `#:` lead comment
  just above the definition, else synthesizes from the name.
- Reconstructs readable function signatures using `ast` only (no imports).
- Deterministic, atomic write. `--check` exits 1 if file differs and prints a diff.

Exit codes:
  0  success / up-to-date
  1  --check mismatch
  2  unexpected error
"""
from __future__ import annotations

import argparse
import ast
import difflib
import os
from pathlib import Path
import sys
from typing import Dict, List, Optional, Tuple, Union
import re
from datetime import datetime, timezone

# ------------------------------- CLI & Constants -------------------------------
REPO_ROOT = Path(__file__).resolve().parents[2]  # repo/
DEFAULT_OUT = REPO_ROOT / "context/available_functions.md"
DEFAULT_SCAN = ["app/"]
ELLIPSIS = "\u2026"  # …
MAX_SUMMARY_LEN = 100

ACRONYM_MAP = {
    "dwd": "DWD",
    "cdc": "CDC",
    "urf": "URF",
}

# --- Deterministic header helpers (avoid timestamp churn in pre-commit) ---
LAST_UPDATED_RE = re.compile(r"^> \*\*Last updated:\*\* .+$", re.M)
def _stable_last_updated() -> str:
    """Date-only (UTC) to avoid minute-by-minute churn and locale-dependent strings."""
    return datetime.now(timezone.utc).date().isoformat()  # e.g., 2025-08-31

def _normalize_for_compare(text: str) -> str:
    """Ignore the 'Last updated' header when comparing current vs generated."""
    return LAST_UPDATED_RE.sub("> **Last updated:** <DATE>", text)

# ------------------------------- Utilities ------------------------------------

def is_hidden(path: Path) -> bool:
    return any(p.startswith(".") for p in path.parts)


def should_skip_dir(d: Path) -> bool:
    return d.name in {"__pycache__", "tests"} or d.name.startswith(".")


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="latin-1")


def ensure_period(s: str) -> str:
    s = s.rstrip()
    if not s:
        return s
    if s[-1] not in ".!?":
        s += "."
    return s


def trim_summary(s: str) -> str:
    s = re.sub(r"\s+", " ", s.strip())
    if len(s) <= MAX_SUMMARY_LEN:
        return ensure_period(s)
    return ensure_period(s[: MAX_SUMMARY_LEN - 1] + ELLIPSIS)


# replace the body of synthesize_from_name with this version
def synthesize_from_name(name: str) -> str:
    if not name:
        return "Public entry point."
    # Split on underscores and CamelCase, preserving leading ALLCAPS runs
    tokens = re.findall(r"[A-Z]{2,}(?=[A-Z][a-z]|$)|[A-Z]?[a-z]+|\d+", name)
    tokens = [t for name_part in name.split("_") for t in re.findall(
        r"[A-Z]{2,}(?=[A-Z][a-z]|$)|[A-Z]?[a-z]+|\d+", name_part)] if "_" in name else tokens
    # Map common acronyms, lowercase everything else
    mapped = []
    for t in tokens:
        key = t.lower()
        mapped.append(ACRONYM_MAP.get(key, t if t.isupper() else t.lower()))
    phrase = " ".join(mapped)
    return ensure_period(phrase[:1].upper() + phrase[1:])


# --------------------------- AST Introspection ---------------------------------

class ModuleAPI(ast.NodeVisitor):
    """Collect public functions/classes according to __all__ or naming rules."""

    def __init__(self, src: str, path: Path) -> None:
        self.src = src
        self.tree = ast.parse(src)
        self.public_names: Optional[set[str]] = None
        self.defs: Dict[str, Union[ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef]] = {}
        self._collect_public_names()
        self._collect_defs()
        self.path = path

    def _collect_public_names(self) -> None:
        exported: Optional[set[str]] = None
        for node in self.tree.body:
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == "__all__":
                        names: List[str] = []
                        if isinstance(node.value, (ast.List, ast.Tuple)):
                            for elt in node.value.elts:
                                if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                                    names.append(elt.value)
                        exported = set(n for n in names if not n.startswith("_"))
        self.public_names = exported

    def _collect_defs(self) -> None:
        for node in self.tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                self.defs[node.name] = node

    def is_public(self, name: str) -> bool:
        if self.path.match("app/tools/**"):
            # tools policy: must have __all__, else skip module entirely
            if self.public_names is None:
                return False
            return name in self.public_names
        if self.public_names is not None:
            return name in self.public_names
        return not name.startswith("_")

def firstline_docstring(node: Union[ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef], src: str) -> Optional[str]:
    doc = ast.get_docstring(node, clean=True)
    if doc:
        return doc.strip().splitlines()[0].strip()
    return None

def leading_colon_comment_before(node: ast.AST, src_lines: List[str]) -> Optional[str]:
    if not hasattr(node, "lineno"):
        return None
    idx = max(0, int(getattr(node, "lineno", 1)) - 2)
    if 0 <= idx < len(src_lines):
        line = src_lines[idx].lstrip()
        if line.startswith("#:"):
            return line[2:].strip()
    return None

def format_annotation(node: Optional[ast.AST], src: str) -> Optional[str]:
    if node is None:
        return None
    try:
        text = ast.get_source_segment(src, node)
        if text:
            return text.strip()
    except Exception:
        pass
    return "T"

def format_default(_) -> str:
    return f"={ELLIPSIS}"

def format_signature(fn: Union[ast.FunctionDef, ast.AsyncFunctionDef], src: str) -> str:
    args = fn.args
    parts: List[str] = []

    def fmt_arg(a: ast.arg, default: Optional[str] = None) -> str:
        ann = format_annotation(a.annotation, src)
        s = a.arg
        if ann:
            s += f": {ann}"
        if default is not None:
            s += default
        return s

    posonly = list(args.posonlyargs)
    defaults = list(args.defaults)
    pos_args = list(args.args)
    all_pos = posonly + pos_args
    num_defaults = len(defaults)
    pos_defaults: List[Optional[str]] = [None] * (len(all_pos) - num_defaults) + [format_default(d) for d in defaults]

    for i, a in enumerate(posonly):
        parts.append(fmt_arg(a, pos_defaults[i]))
    if posonly:
        parts.append("/")

    for j, a in enumerate(pos_args, start=len(posonly)):
        parts.append(fmt_arg(a, pos_defaults[j]))

    if args.vararg is not None:
        s = "*" + args.vararg.arg
        ann = format_annotation(args.vararg.annotation, src)
        if ann:
            s += f": {ann}"
        parts.append(s)
    else:
        if args.kwonlyargs:
            parts.append("*")

    for i, a in enumerate(args.kwonlyargs):
        default = None
        if args.kw_defaults and args.kw_defaults[i] is not None:
            default = format_default(args.kw_defaults[i])
        parts.append(fmt_arg(a, default))

    if args.kwarg is not None:
        s = "**" + args.kwarg.arg
        ann = format_annotation(args.kwarg.annotation, src)
        if ann:
            s += f": {ann}"
        parts.append(s)

    sig = f"({', '.join(parts)})"
    ret_ann = format_annotation(getattr(fn, "returns", None), src)
    if ret_ann:
        sig += f" -> {ret_ann}"
    return sig

# ----------------------------- Rendering ---------------------------------------

def render_markdown(index: Dict[Path, List[Tuple[str, str]]]) -> str:
    lines: List[str] = []  
    # Stable, locale-free date to keep file identical across quick reruns  
    ts = _stable_last_updated()
    lines.append("# Available Functions (Reference Index)")
    lines.append("")
    lines.append(
        "> Scope: This index lists **only the callable entry points other modules should use**.  ")
    lines.append(
        "> Keep it short and stable. Omit private helpers and internal class methods.  ")
    lines.append(f"> **Last updated:** {ts}")
    lines.append("")
    lines.append("---")
    lines.append("")

    for mod_path in sorted(index.keys(), key=lambda p: str(p)):
        lines.append(f"## {mod_path.as_posix()}")
        entries = sorted(index[mod_path], key=lambda t: t[0].lower())
        for name_sig, summary in entries:
            lines.append(f"- `{name_sig}` — {summary}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"

# ------------------------------ Scanner ----------------------------------------

def scan_paths(globs: List[str]) -> List[Path]:
    files: List[Path] = []
    for pattern in globs:
        base = REPO_ROOT / pattern
        if base.is_file() and base.suffix == ".py":
            files.append(base)
        elif base.is_dir():
            for path in base.rglob("*.py"):
                if any(should_skip_dir(parent) for parent in path.parents):
                    continue
                if is_hidden(path):
                    continue
                files.append(path)
    return sorted(set(files))

def build_index(py_files: List[Path]) -> Tuple[Dict[Path, List[Tuple[str, str]]], Dict[str, int]]:
    index: Dict[Path, List[Tuple[str, str]]] = {}
    counts = {"modules": 0, "functions": 0, "classes": 0, "skipped_private": 0}

    for file in py_files:
        if any(part == "tests" for part in file.parts):
            continue
        rel = file.relative_to(REPO_ROOT)
        if rel.parts[0] != "app":
            continue
        src = read_text(file)
        try:
            api = ModuleAPI(src, rel)
        except SyntaxError:
            continue
        counts["modules"] += 1

        src_lines = src.splitlines()
        entries: List[Tuple[str, str]] = []

        for name, node in api.defs.items():
            if not api.is_public(name):
                counts["skipped_private"] += 1
                continue
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                sig = format_signature(node, src)
                display = f"{name}{sig}"
                counts["functions"] += 1
            elif isinstance(node, ast.ClassDef):
                display = name
                counts["classes"] += 1
            else:
                continue

            desc = firstline_docstring(node, src)
            if not desc:
                desc = leading_colon_comment_before(node, src_lines)
            if not desc:
                desc = synthesize_from_name(name)
            summary = trim_summary(desc)

            entries.append((display, summary))

        if entries:
            index.setdefault(rel, []).extend(entries)

    return index, counts

# ------------------------------- Main ------------------------------------------

def write_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8", newline="\n")
    os.replace(tmp, path)

def run(paths: List[str], check: bool) -> int:
    try:
        py_files = scan_paths(paths)
        index, counts = build_index(py_files)
        out = render_markdown(index)

        if check and DEFAULT_OUT.exists():
            current = read_text(DEFAULT_OUT)
            # Compare ignoring the 'Last updated' header to prevent false diffs
            if _normalize_for_compare(current) != _normalize_for_compare(out):
                diff = difflib.unified_diff(
                    current.splitlines(), out.splitlines(),
                    fromfile=str(DEFAULT_OUT), tofile=str(DEFAULT_OUT), lineterm=""
                )
                print("\n".join(diff))
                print(f"\n[gen_available_functions] modules={counts['modules']} functions={counts['functions']} classes={counts['classes']} skipped={counts['skipped_private']}")
                return 1
            else:
                print(f"Up to date. modules={counts['modules']} functions={counts['functions']} classes={counts['classes']} skipped={counts['skipped_private']}")
                return 0
        else:
            write_atomic(DEFAULT_OUT, out)
            print(f"Wrote {DEFAULT_OUT} (modules={counts['modules']} functions={counts['functions']} classes={counts['classes']} skipped={counts['skipped_private']})")
            return 0
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Generate context/available_functions.md from codebase")
    p.add_argument("--check", action="store_true", help="Exit 1 if the generated content differs from the current file")
    p.add_argument("--paths", nargs="*", default=DEFAULT_SCAN, help="Globs to narrow scope (default: app/)")
    args = p.parse_args(argv)
    return run(args.paths, args.check)

if __name__ == "__main__":
    sys.exit(main())

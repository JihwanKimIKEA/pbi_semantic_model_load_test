"""Generate ModelCompare_LoadTest.ipynb from ModelCompare_LoadTest.py.

The .py file is the source of truth. Edit it, then regenerate the notebook:

    python build_notebook.py            # (re)writes ModelCompare_LoadTest.ipynb
    python build_notebook.py --check    # exit 1 if the .ipynb is out of sync

--py PATH / --ipynb PATH override the default files next to this script.

Every cell in the .py starts with a marker block:

    # ==============================================================================
    # CELL 4 | code | hidden | Step 2 - Setup and helpers
    # ==============================================================================

Fields: index | type | tag | title
  index  0, 1, 2, ... in file order (renumber the later cells when inserting one)
  type   markdown or code
  tag    "parameters" (the Fabric parameter cell), "hidden" (code collapsed) or "-"
  title  code cells start with "# <title>"; for markdown it is only a label
Markdown bodies are lines "# text" (a bare "#" is an empty line); "# " is dropped.

Cell ids and the notebook metadata are kept from the existing .ipynb. A cell keeps
its id when its type and key match an existing cell; the key is the title (code) or
the first line (markdown) without a leading "Step <n> - " / "## Step <n> - ", so
renumbering steps keeps the ids. New cells get id = sha1(key)[:8].
"""
import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
MARKER = re.compile(r"^# =+\n# CELL (.*)\n# =+\n", re.M)
STEP_PREFIX = re.compile(r"^(#+ )?Step \d+ - ")
TAGS = ("-", "parameters", "hidden")
DEFAULT_NOTEBOOK = {  # used when there is no .ipynb yet
    "metadata": {
        "dependencies": {"lakehouse": {}},
        "kernel_info": {"jupyter_kernel_name": "python3.11", "name": "jupyter"},
        "kernelspec": {"display_name": "Jupyter", "language": "Jupyter", "name": "jupyter"},
        "language_info": {"name": "python"},
        "microsoft": {
            "language": "python",
            "language_group": "jupyter_python",
            "ms_spell_check": {"ms_spell_check_language": "en"},
        },
        "nteract": {"version": "nteract-front-end@1.0.0"},
        "sessionKeepAliveTimeout": 0,
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}


def parse_py(text):
    """Split the .py into cells: [(cell_type, tag, title, source_text)]."""
    text = text.replace("\r\n", "\n")
    marks = list(MARKER.finditer(text))
    if not marks:
        raise ValueError("no '# CELL <index> | <type> | <tag> | <title>' markers found")
    cells = []
    for i, m in enumerate(marks):
        where = "line %d" % (text.count("\n", 0, m.start()) + 2)
        fields = [f.strip() for f in m.group(1).split("|", 3)]
        if len(fields) != 4:
            raise ValueError(f"{where}: expected '# CELL <index> | <type> | <tag> | <title>'")
        index, cell_type, tag, title = fields
        if index != str(i):
            raise ValueError(f"{where}: CELL index is {index!r}, expected {i} (indices must be 0..N-1 in order)")
        if cell_type not in ("markdown", "code"):
            raise ValueError(f"{where}: cell type {cell_type!r} must be 'markdown' or 'code'")
        if tag not in TAGS or (cell_type == "markdown" and tag != "-"):
            raise ValueError(f"{where}: tag {tag!r} must be one of {TAGS} ('-' for markdown)")
        end = marks[i + 1].start() if i + 1 < len(marks) else len(text)
        lines = text[m.end():end].split("\n")
        while lines and not lines[0].strip():
            lines.pop(0)
        while lines and not lines[-1].strip():
            lines.pop()
        if cell_type == "code":
            lines = ["# " + title] + lines
        else:
            for n, line in enumerate(lines):
                if line.startswith("# "):
                    lines[n] = line[2:]
                elif line.rstrip() in ("#", ""):
                    lines[n] = ""
                else:
                    raise ValueError(f"{where}: markdown line does not start with '# ': {line!r}")
        cells.append((cell_type, tag, title, "\n".join(lines)))
    return cells


def cell_key(cell_type, text):
    """Stable key used to match a cell to the existing notebook (survives renumbering)."""
    first = text.split("\n", 1)[0]
    if cell_type == "code" and first.startswith("# "):
        first = first[2:]
    return STEP_PREFIX.sub("", first)


def assign_ids(keys, old_cells):
    """Reuse the old id of each (cell_type, key); new cells get sha1(key)[:8]."""
    pool = {}
    for c in old_cells:
        src = c.get("source", "")
        text = src if isinstance(src, str) else "".join(src)
        if c.get("id"):
            pool.setdefault((c["cell_type"], cell_key(c["cell_type"], text)), []).append(c["id"])
    ids = [pool[k].pop(0) if pool.get(k) else None for k in keys]
    used = set(filter(None, ids))
    for n, (k, cid) in enumerate(zip(keys, ids)):
        if cid is None:
            seed, i = k[1], 0
            cid = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:8]
            while cid in used:
                i += 1
                cid = hashlib.sha1(f"{seed}#{i}".encode("utf-8")).hexdigest()[:8]
            ids[n] = cid
            used.add(cid)
    return ids


def as_source(text):
    """Jupyter stores source as a list of lines, each ending in "\\n" except the last."""
    lines = text.split("\n")
    return [line + "\n" for line in lines[:-1]] + [lines[-1]] if text else []


def build_notebook(cells, old):
    keys = [(cell_type, cell_key(cell_type, text)) for cell_type, _, _, text in cells]
    ids = assign_ids(keys, old.get("cells", []))
    out = []
    for (cell_type, tag, _, text), cid in zip(cells, ids):
        if cell_type == "markdown":
            out.append({"cell_type": "markdown", "id": cid,
                        "metadata": {"nteract": {"transient": {"deleting": False}}},
                        "source": as_source(text)})
            continue
        meta = {"microsoft": {"language": "python", "language_group": "jupyter_python"}}
        if tag == "parameters":
            meta["tags"] = ["parameters"]
        elif tag == "hidden":
            meta["jupyter"] = {"outputs_hidden": False, "source_hidden": True}
        out.append({"cell_type": "code", "execution_count": None, "id": cid,
                    "metadata": meta, "outputs": [], "source": as_source(text)})
    return {"cells": out,
            "metadata": old.get("metadata", DEFAULT_NOTEBOOK["metadata"]),
            "nbformat": old.get("nbformat", DEFAULT_NOTEBOOK["nbformat"]),
            "nbformat_minor": old.get("nbformat_minor", DEFAULT_NOTEBOOK["nbformat_minor"])}


def first_difference(new, old, cells):
    if len(new["cells"]) != len(old.get("cells", [])):
        return f"cell count differs: {len(new['cells'])} vs {len(old.get('cells', []))} (.py vs .ipynb)"
    for i, (a, b) in enumerate(zip(new["cells"], old["cells"])):
        if a != b:
            return f"cell {i} differs ({cells[i][2]})"
    return "notebook metadata or JSON formatting differs"


def main(argv=None):
    ap = argparse.ArgumentParser(description="Generate the .ipynb from the .py (the source of truth).")
    ap.add_argument("--py", type=Path, metavar="PATH", default=HERE / "ModelCompare_LoadTest.py",
                    help="source .py (default: ModelCompare_LoadTest.py next to this script)")
    ap.add_argument("--ipynb", type=Path, metavar="PATH", default=HERE / "ModelCompare_LoadTest.ipynb",
                    help="notebook to write/check (default: ModelCompare_LoadTest.ipynb next to this script)")
    ap.add_argument("--check", action="store_true", help="only compare; exit 1 if out of sync")
    args = ap.parse_args(argv)

    try:
        cells = parse_py(args.py.read_bytes().decode("utf-8-sig"))
    except ValueError as e:
        sys.exit(f"error: {args.py}: {e}")
    old_text = args.ipynb.read_text(encoding="utf-8-sig") if args.ipynb.exists() else None
    old = json.loads(old_text) if old_text else {}
    nb = build_notebook(cells, old)
    new_text = json.dumps(nb, indent=1, ensure_ascii=False) + "\n"

    if args.check:
        if old_text is None:
            print(f"{args.ipynb}: missing; run build_notebook.py to create it")
            return 1
        if new_text == old_text.replace("\r\n", "\n"):
            print(f"{args.ipynb}: in sync ({len(cells)} cells)")
            return 0
        print(f"{args.ipynb}: out of sync, {first_difference(nb, old, cells)}; run build_notebook.py")
        return 1
    args.ipynb.write_bytes(new_text.encode("utf-8"))
    print(f"Wrote {args.ipynb}: {len(cells)} cells")
    return 0


if __name__ == "__main__":
    sys.exit(main())

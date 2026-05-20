"""Generate the examples pages and navigation."""

import os
import re
from pathlib import Path

import mkdocs_gen_files

nav = mkdocs_gen_files.Nav()
nav["Introduction to the Examples"] = "introduction.md"

root = Path(__file__).parent.parent
src = root / "examples"


for directory in [Path(d.path) for d in os.scandir(src)]:
    example_name = None

    for path in sorted(directory.rglob("*.md"), key=lambda s: "aaa" if s.name == "README.md" else s.name):
        module_path = path.relative_to(src).with_suffix("")
        doc_path = path.relative_to(src)
        full_doc_path = Path("examples", doc_path)

        parts = tuple(module_path.parts)

        with open(path) as f:
            override_name = re.findall(r"^#\s*([^#].*)\s*$", f.readline())

        if parts[-1] == "README":
            example_name = tuple(override_name)
            override_name = ("Overview",)
        if example_name:
            parts = example_name + parts[1:]

        if override_name:
            nav[parts[:-1] + override_name] = doc_path.as_posix()
        else:
            nav[parts] = doc_path.as_posix()

        with mkdocs_gen_files.open(full_doc_path, "w") as fd:
            fd.write(path.read_text())

        mkdocs_gen_files.set_edit_path(full_doc_path, path.relative_to(root))

    for path in sorted(directory.rglob("*.png")):
        module_path = path.relative_to(src).with_suffix("")
        doc_path = path.relative_to(src)
        full_doc_path = Path("examples", doc_path)

        with mkdocs_gen_files.open(full_doc_path, "wb") as fd:
            fd.write(path.read_bytes())

    for path in sorted(directory.rglob("*.py")):
        module_path = path.relative_to(src).with_suffix("")
        module_path_src = path.relative_to(src).with_suffix(".src")
        doc_path = path.relative_to(src).with_suffix(".md")
        doc_path_src = path.relative_to(src).with_suffix(".src.md")
        full_doc_path = Path("examples", doc_path)
        full_doc_path_src = Path("examples", doc_path_src)

        parts = tuple(module_path.parts)
        src_parts = tuple(module_path_src.parts)
        if example_name:
            nav[(*example_name, "Documentation", *parts[1:-1], f"{parts[-1]}.py")] = doc_path.as_posix()
            nav[(*example_name, "Source Files", *parts[1:-1], f"{parts[-1]}.py")] = doc_path_src.as_posix()
        else:
            nav[parts] = doc_path.as_posix()
            nav[src_parts] = doc_path_src.as_posix()

        with mkdocs_gen_files.open(full_doc_path, "w") as fd:
            ident = ".".join(parts)
            fd.write(
                f"""::: {ident}
            options:
                members_order: source
            """
            )
        with mkdocs_gen_files.open(full_doc_path_src, "w") as fd:
            fd.write("---\nhide:\n  - toc\n---\n")
            fd.write(f"```\n{path.read_text()}\n```\n")

        mkdocs_gen_files.set_edit_path(full_doc_path, path.relative_to(root))

with mkdocs_gen_files.open("examples/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())

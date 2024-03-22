import os
import re
import difflib
from mkdocs.plugins import get_plugin_logger
from mkdocs.config.defaults import MkDocsConfig
from mkdocs.structure.files import Files
from mkdocs.structure.pages import Page


log = get_plugin_logger(os.path.basename(__file__))


def on_page_markdown(markdown: str, page: Page, config: MkDocsConfig, files: Files):
    if page.file.src_uri != "architecture/system_tables.md":
        return

    capture = None
    parsed_spec = list()
    lines: list[str] = re.sub("<!--.*?-->", "", markdown, flags=re.DOTALL).split("\n")
    for line in lines:
        if line.startswith("###"):
            parsed_spec.extend(["", line])
            capture = None
            continue
        elif line == "Поля:":
            parsed_spec.extend(["", line, ""])
            capture = "format"
            continue
        elif line == "Индексы:":
            parsed_spec.extend(["", line, ""])
            capture = "index"
            continue

        if capture == "format":
            if line.startswith("*"):
                match = re.search(r"(\* `[a-z_]+`: \([a-z_]+)[,\)]", line)
                parsed_spec.append(match[1] + ")" if match else line)
        elif capture == "index":
            if line.startswith("*"):
                parsed_spec.append(line)

    with open("system_tables.spec", "r") as f:
        expected_spec = f.read().split("\n")

    parsed_spec.append("")

    diff = difflib.unified_diff(
        parsed_spec,
        expected_spec,
        fromfile="architecture/system_tables.md",
        tofile="system_tables.spec",
        n=10,
    )

    first_line = next(diff, None)

    if first_line is None:
        return

    log.warning("SYSTEM TABLES DIFFERS!\n" + f"{first_line}\n" + "\n".join(diff))

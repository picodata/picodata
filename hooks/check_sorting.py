import os
import re
import difflib
from mkdocs.plugins import get_plugin_logger
from mkdocs.config.defaults import MkDocsConfig
from mkdocs.structure.files import Files
from mkdocs.structure.pages import Page


log = get_plugin_logger(os.path.basename(__file__))


def on_page_markdown(markdown: str, page: Page, config: MkDocsConfig, files: Files):
    if page.file.src_uri == "reference/cli.md":
        return reference_cli(markdown, page)
    elif page.file.src_uri == "reference/audit_events.md":
        return reference_audit_events(markdown, page)
    elif page.file.src_uri == "architecture/rpc_api.md":
        return reference_rpc_api(markdown, page)


def reference_cli(markdown: str, page: Page):
    lines: list[str] = re.sub("<!--.*?-->", "", markdown, flags=re.DOTALL).split("\n")
    headers: list[str] = list(filter(lambda line: re.match("^##+ ", line), lines))

    last_h2: str = ""
    index: dict[str, list[str]] = dict()

    for h in headers:
        if h.startswith("## "):
            last_h2 = h
            index[last_h2] = []
        elif h.startswith("### ") and "--" in h:
            arg = re.findall(r"--[\w-]+", h)[0]
            index[last_h2].append(arg)

    for h2, args in index.items():
        args_sorted = sorted(args)
        if args != args_sorted:
            log.warning(f"INCORRECT SORTING @ {page.file.src_uri}: {h2}")
            diff = difflib.unified_diff(args, args_sorted)
            log.info("\n" + "\n".join(diff))


def reference_audit_events(markdown: str, page: Page):
    lines: list[str] = re.sub("<!--.*?-->", "", markdown, flags=re.DOTALL).split("\n")
    h3: list[str] = list(filter(lambda line: line.startswith("### "), lines))

    h3_sorted = sorted(h3)
    if h3 != h3_sorted:
        log.warning(f"INCORRECT SORTING @ {page.file.src_uri}")
        diff = difflib.unified_diff(h3, h3_sorted)
        log.info("\n" + "\n".join(diff))

def reference_rpc_api(markdown: str, page: Page):
    lines: list[str] = re.sub("<!--.*?-->", "", markdown, flags=re.DOTALL).split("\n")
    headers: list[str] = list(filter(lambda line: re.match("^##+ ", line), lines))

    last_h2: str = ""
    index: dict[str, list[str]] = dict()

    for h in headers:
        if h.startswith("## "):
            last_h2 = h
            index[last_h2] = []
        elif h.startswith("### ") and ".proc" in h:
            arg = re.findall(r"\.[\w_]+", h)[0]
            index[last_h2].append(arg)

    for h2, args in index.items():
        if "Service API" not in h2:
            continue
        args_sorted = sorted(args)
        if args != args_sorted:
            log.warning(f"INCORRECT SORTING @ {page.file.src_uri}: {h2}")
            diff = difflib.unified_diff(args, args_sorted)
            log.info("\n" + "\n".join(diff))

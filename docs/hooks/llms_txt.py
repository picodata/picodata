"""The docs for AI assistants: llms.txt, llms-full.txt and a Markdown copy of every page.

An assistant that is asked about Picodata reads pages the way a crawler does, and an HTML page
is mostly navigation and markup to it. https://llmstxt.org/ describes what to publish instead:

- llms.txt at the root of the site: a short index, the sections of the navigation with a link
  to every page, in Markdown;
- the pages themselves in Markdown: here index.md next to every index.html, and the links of
  llms.txt point at these copies. Pages outside the navigation get a copy too, other pages
  link to them;
- llms-full.txt: all the pages in one file, for tools that take the docs whole.

Every docs version gets its own set: the files land in the root of the version, such as
/picodata/devel/llms.txt. The Markdown is the page source after the other hooks have processed
it, so the syntax of the extensions (admonitions, heading anchors) stays as the authors wrote it.
Relative links are rewritten: a copy lies one directory deeper than its source, and a link to
another page has to lead to the copy of that page.
"""

import os
import posixpath
import re

from mkdocs.config.defaults import MkDocsConfig
from mkdocs.structure.files import Files
from mkdocs.structure.nav import Navigation, Section
from mkdocs.structure.pages import Page

SUMMARY = (
    "Picodata — распределённая СУБД с открытым исходным кодом: хранение данных в памяти и на "
    "диске, распределённый SQL, совместимость с протоколом PostgreSQL, расширение плагинами на "
    "Rust. Здесь документация одной версии Picodata; ссылки ведут на страницы в формате Markdown."
)

# Inline links and images: [text](target), ![alt](target "title").
LINK = re.compile(r"(\]\()([^)\s]+)")

# page source path -> the page and its Markdown
sources: dict[str, tuple[Page, str]] = {}
navigation: list[Navigation] = []
site_files: list[Files] = []


def on_nav(nav: Navigation, config: MkDocsConfig, files: Files) -> None:
    sources.clear()
    navigation[:] = [nav]
    site_files[:] = [files]


def on_page_markdown(markdown: str, page: Page, config: MkDocsConfig, files: Files) -> None:
    sources[page.file.src_uri] = (page, markdown)


def markdown_url(page: Page, config: MkDocsConfig) -> str:
    """Address of the Markdown copy: the page lives in a directory, the copy is its index.md."""
    return posixpath.join(config.site_url or "/", page.url, "index.md")


def relink(markdown: str, page: Page) -> str:
    """Point relative links at where their targets are published, as seen from the copy."""
    here = posixpath.dirname(page.file.src_uri)

    def target(m: re.Match[str]) -> str:
        path, sep, anchor = m.group(2).partition("#")
        if not path or path.startswith("/") or re.match(r"[a-z][a-z0-9+.-]*:", path, re.I):
            return m.group(0)
        file = site_files[0].get_file_from_path(posixpath.normpath(posixpath.join(here, path)))
        if file is None:
            return m.group(0)
        url = posixpath.join(file.url, "index.md") if file.is_documentation_page() else file.url
        return m.group(1) + posixpath.relpath(url, page.url or ".") + sep + anchor

    return LINK.sub(target, markdown)


def pages_of(item: Section | Page, path: tuple[str, ...] = ()) -> list[tuple[str, Page]]:
    """Pages of a navigation item with the titles of the subsections leading to each."""
    if isinstance(item, Page):
        return [(" / ".join([*path, item.title or ""]), item)]
    found = []
    for child in getattr(item, "children", None) or []:
        if isinstance(child, Section):
            found += pages_of(child, (*path, child.title))
        elif isinstance(child, Page):
            found += pages_of(child, path)
    return found


def on_post_build(config: MkDocsConfig) -> None:
    texts = {uri: relink(markdown, page).strip() for uri, (page, markdown) in sources.items()}
    for uri, (page, _) in sources.items():
        copy = os.path.join(os.path.dirname(page.file.abs_dest_path), "index.md")
        with open(copy, "w", encoding="utf-8") as f:
            f.write(texts[uri] + "\n")

    index = [f"# {config.site_name}", "", f"> {SUMMARY}", ""]
    full = []
    for item in navigation[0].items:
        listed = [(title, page) for title, page in pages_of(item) if page.file.src_uri in texts]
        if not listed:
            continue
        index += [f"## {item.title}", ""]
        index += [f"- [{title}]({markdown_url(page, config)})" for title, page in listed]
        index.append("")
        full += [
            f"<!-- {markdown_url(page, config)} -->\n\n{texts[page.file.src_uri]}\n"
            for _, page in listed
        ]

    for name, lines in (("llms.txt", index), ("llms-full.txt", full)):
        with open(os.path.join(config.site_dir, name), "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

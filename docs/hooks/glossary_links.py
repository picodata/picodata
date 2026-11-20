"""Tooltips for glossary terms everywhere in the text, not only where the author put a link.

The theme shows a preview of a glossary section when the reader hovers a link to it
(material.extensions.preview in mkdocs.yml). This hook adds such links at build time: it wraps
the first mention of a term in every section of a page, except on the page that is named after
the term. The page sources stay as they are. On touch devices there is no hover, so a script
in overrides/main.html makes these links toggle the tooltip on tap instead of following them.

Which words are a term is taken from the docs themselves: the texts of the links the authors
already point at a glossary section ("инстанса", "инстансов" for #instance) and the section
title. A link text counts only when it reads as a form of the title, word for word, so
"см. подробнее", "строгой" for "Строгая согласованность" or "развёртывания кластера" for
#bootstrap do not become terms. Headings that only group other headings are not terms either.

The glossary of a plugin (plugins/<name>/.../glossary.md) applies to the pages of that plugin
and wins there over the common one: "бакет" means one thing in S3R and another in the core.
"""

import posixpath
import re
from collections import Counter, defaultdict

from mkdocs.config.defaults import MkDocsConfig
from mkdocs.structure.files import Files
from mkdocs.structure.pages import Page
from mkdocs.utils import get_relative_url

GLOSSARY_NAME = "glossary.md"
HEADING = re.compile(r"^(#{2,6})\s+(.+?)\s*\{:\s*#([\w-]+)\s*\}\s*$", re.M)
LINK = re.compile(r"\[([^\]\[]+)\]\(([^)\s#]*)#([\w-]+)\)")
WORD = re.compile(r"\w+")
# A tag may hold ">" inside a quoted attribute value.
TOKEN = re.compile(r"<!--.*?-->|<(?:\"[^\"]*\"|'[^']*'|[^'\">])*>", re.S)
TAG = re.compile(r"<(/?)([a-zA-Z][\w-]*)")
GLOSSARY_HREF = re.compile(r"""href=["'][^"'#]*glossary/#([\w-]+)["']""")
# Text inside these is not prose: a link there would break a heading anchor, a code sample or
# another link.
SKIP_TAGS = {
    "a",
    "button",
    "code",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "kbd",
    "label",
    "pre",
    "script",
    "style",
    "summary",
    "svg",
    "th",
}
SKIP_CLASS = "admonition-title"
SECTION_TAGS = {"h1", "h2"}
# Sections whose title is also an everyday word with another meaning: "окно терминала",
# "каталог LDAP", "манифест Kubernetes", "блок tls", "в таблице ниже". Their terms get a tooltip
# only where the author links them by hand.
AMBIGUOUS = {"block", "catalog", "manifest", "position", "table", "window"}

# glossary page -> term as written in the text, lowercase -> anchor of its section
terms: dict[str, dict[str, str]] = {}
# glossary page -> anchor -> section title
titles: dict[str, dict[str, str]] = {}
urls: dict[str, str] = {}
scopes: dict[str, tuple[re.Pattern[str] | None, dict[str, tuple[str, str]]]] = {}


def normalize(text: str) -> str:
    # Emphasis marks go, an underscore inside a name ("replication_factor") stays.
    text = re.sub(r"[*`]|(?<!\w)_+|_+(?!\w)", "", text)
    return re.sub(r"\s+", " ", text).strip().lower()


def names(title: str) -> list[str]:
    """Names a section title gives the term: "Тир (tier)" is both "Тир" and "tier"."""
    aliases = re.findall(r"\((.*?)\)", title)
    return [normalize(n) for n in [re.sub(r"\(.*?\)", "", title), *aliases] if n.strip()]


def looks_like(form: str, title: str) -> bool:
    """The form is one of the names of the title, word for word, in another grammatical form."""
    words = WORD.findall(form)
    return any(
        len(words) == len(name) and all(w.startswith(n[:4]) for w, n in zip(words, name))
        for name in (WORD.findall(n) for n in names(title))
    )


def sections(text: str) -> dict[str, str]:
    """Anchor -> title of every glossary section that defines a term."""
    found = list(HEADING.finditer(text))
    result = {}
    for i, m in enumerate(found):
        following = found[i + 1] if i + 1 < len(found) else None
        start, end = m.end(), following.start() if following else len(text)
        body = text[start:end]
        # No text of its own and a deeper heading right after: the heading groups terms.
        if following and not body.strip() and len(following.group(1)) > len(m.group(1)):
            continue
        if m.group(3) not in AMBIGUOUS:
            result[m.group(3)] = m.group(2)
    return result


def root(glossary: str) -> str:
    """The part of the site a glossary covers: its plugin, or everything."""
    parts = glossary.split("/")
    return "/".join(parts[:2]) + "/" if parts[0] == "plugins" and len(parts) > 2 else ""


def on_files(files: Files, config: MkDocsConfig) -> None:
    terms.clear()
    titles.clear()
    urls.clear()
    scopes.clear()
    sources = {f.src_uri: f.content_string for f in files.documentation_pages()}
    for uri, text in sources.items():
        if posixpath.basename(uri) == GLOSSARY_NAME:
            titles[uri] = sections(text)
            urls[uri] = files.get_file_from_path(uri).url  # type: ignore[union-attr]

    counts: dict[str, dict[str, Counter[str]]] = {g: defaultdict(Counter) for g in titles}
    linked: set[tuple[str, str]] = set()
    for uri, text in sources.items():
        for label, path, anchor in LINK.findall(text):
            target = posixpath.normpath(posixpath.join(posixpath.dirname(uri), path))
            target = target if path else uri
            if anchor not in titles.get(target, {}):
                continue
            linked.add((target, anchor))
            form = normalize(label)
            if looks_like(form, titles[target][anchor]):
                counts[target][form][anchor] += 1
    for glossary, defined in titles.items():
        for anchor, title in defined.items():
            # A section nobody links to ("Общие сведения") is not used as a term in the docs.
            # The English name in parentheses ("term", "index") is too common a word to add
            # unless an author has used it as a link text.
            if (glossary, anchor) in linked:
                counts[glossary][names(title)[0]][anchor] += 1

    for glossary, forms in counts.items():
        terms[glossary] = {}
        for form, anchors in forms.items():
            (best, n), *rest = anchors.most_common(2)
            # The same word used for two sections about equally often is not a term of either.
            if len(form) >= 3 and not (rest and rest[0][1] * 2 > n):
                terms[glossary][form] = best


def scope(page: str) -> tuple[re.Pattern[str] | None, dict[str, tuple[str, str]]]:
    """Terms that apply to a page: the common glossary, then the one of its plugin on top."""
    key = next((root(g) for g in terms if root(g) and page.startswith(root(g))), "")
    if key not in scopes:
        lookup: dict[str, tuple[str, str]] = {}
        for glossary in sorted(terms, key=lambda g: len(root(g))):
            if page.startswith(root(glossary)):
                lookup.update({form: (glossary, a) for form, a in terms[glossary].items()})
        forms = sorted(lookup, key=len, reverse=True)
        alternatives = "|".join(re.escape(f).replace(r"\ ", r"\s+") for f in forms)
        # A term inside a longer hyphenated word ("raft" in "raft-группа") is another term.
        pattern = re.compile(rf"(?<![\w-])(?:{alternatives})(?![\w-])", re.I) if forms else None
        scopes[key] = (pattern, lookup)
    return scopes[key]


def on_page_content(html: str, page: Page, config: MkDocsConfig, files: Files) -> str:
    uri = page.file.src_uri
    pattern, lookup = scope(uri)
    # Glossary pages link their terms to each other by hand.
    if pattern is None or uri in terms:
        return html

    # A page named after a term is about it: a tooltip with its definition there is noise.
    heading = normalize(page.title or "")
    own = {t for t in set(lookup.values()) if looks_like(heading, titles[t[0]][t[1]])}
    seen: set[tuple[str, str]] = set()

    def link_terms(text: str) -> str:
        out, last = [], 0
        for m in pattern.finditer(text):
            target = lookup[normalize(m.group(0))]
            if target in seen or target in own:
                continue
            seen.add(target)
            href = get_relative_url(urls[target[0]], page.url) + "#" + target[1]
            link = f'<a class="glossary-term" data-preview href="{href}">{m.group(0)}</a>'
            start, end = m.span()
            out += [text[last:start], link]
            last = end
        return "".join(out) + text[last:] if out else text

    result, pos = [], 0
    skip_tag, depth = "", 0
    for token in TOKEN.finditer(html):
        start, end = token.span()
        text = html[pos:start]
        result.append(text if skip_tag else link_terms(text))
        result.append(token.group(0))
        pos = end
        tag = TAG.match(token.group(0))
        if not tag or token.group(0).endswith("/>"):
            continue
        closing, name = tag.group(1) == "/", tag.group(2).lower()
        if skip_tag:
            if name == skip_tag:
                depth += -1 if closing else 1
                if depth == 0:
                    skip_tag = ""
            continue
        if closing:
            continue
        if name in SECTION_TAGS:
            seen.clear()
        if name == "a":
            # A link the author made counts as the mention of the term in this section.
            explicit = GLOSSARY_HREF.search(token.group(0))
            if explicit:
                seen.update(t for t in lookup.values() if t[1] == explicit.group(1))
        if name in SKIP_TAGS or SKIP_CLASS in token.group(0):
            skip_tag, depth = name, 1
    result.append(link_terms(html[pos:]) if not skip_tag else html[pos:])
    return "".join(result)

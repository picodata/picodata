import os
import re
from urllib.parse import urlsplit
from mkdocs.plugins import get_plugin_logger
from mkdocs.config.defaults import MkDocsConfig
from mkdocs.structure.files import Files
from mkdocs.structure.pages import Page


log = get_plugin_logger(os.path.basename(__file__))

# Supported links formats:
#
# [text](href)
# ![text](image.svg)
LINKS_INLINE_RE = r"(?s)(!?)\[([^\[\]]*?)\]\((.+?)\)"
#
# [label]: href
LINKS_REFERENCE_RE = r"(?m)^\[(.+?)\]:(?: *\n? *)(\S+?)$"
#
# [label]
# [text][label]
LINKS_LABEL_RE = r"(?s)(?:\[(?! )(.+?)(?<! )\])+"

# See also
#
# Python re — Regular expression operations
# https://docs.python.org/3/library/re.html
#
# Markdown reference-style links
# https://www.markdownguide.org/basic-syntax/#reference-style-links

CODE_BLOCK_RE = r"(?ms)^(?: *)```.+?```$"
CODE_INLINE_RE = r"(?s)`.+?`"
COMMENT_RE = r"(?s)<!--.*?-->"


def check_href(href: str, src_uri: str, config: MkDocsConfig):
    """
    Check if `href` link is correct. A link points to a markdown file,
    optionally to a specific paragraph. Example:

        ../overview/description.md#description

    Links with no path (just a paragraph) relate to `src_uri`. Relative
    paths relate to the directory containing `src_uri`. The `src_uri` is a
    path to a markdown file containing that link.

    The list of existing pages and anchors is defined in `config`, see
    `hooks/check_headers.py` which populates it.

    Return either a string error or `None` if the link is correct.
    """
    assert "\n" not in href
    url = urlsplit(href)

    if url.scheme or url.netloc:
        # Skip external links
        return None

    if not url.path:
        target = src_uri
    else:
        target = os.path.join(os.path.dirname(src_uri), url.path)
        target = os.path.normpath(target)

    if target not in config["anchors"]:
        return f"BROKEN LINK @ {src_uri}"

    if url.fragment and url.fragment not in config["anchors"][target]:
        return f"BROKEN LINK ANCHOR @ {src_uri}"


# https://www.mkdocs.org/dev-guide/plugins/#events
def on_page_markdown(markdown: str, page: Page, config: MkDocsConfig, files: Files):
    markdown = re.sub(COMMENT_RE, "", markdown)
    markdown = re.sub(CODE_BLOCK_RE, "<code block stripped>", markdown)
    markdown = re.sub(CODE_INLINE_RE, "<inline code stripped>", markdown)
    src_uri = page.file.src_uri

    # Check inline-style links
    for match in re.finditer(LINKS_INLINE_RE, markdown):
        link = match[0].replace("\n", " ")
        is_image, _, href = match.groups()

        if is_image:
            # Skip ![text](image.svg)
            continue

        err = check_href(href, src_uri, config)
        if err:
            log.warning(f"{err}: {link}")

    # Cut inline-style links because they are confused with labels
    markdown = re.sub(LINKS_INLINE_RE, "<inline-style link stripped>", markdown)

    reference_links: dict[str, str] = dict()
    used_labels: set[str] = set()

    # Check reference-style links
    for match in re.finditer(LINKS_REFERENCE_RE, markdown):
        link = match[0].replace("\n", " ")
        label, href = match.groups()

        if label not in reference_links:
            reference_links[label] = link
        else:
            log.warning(f"AMBIGUOUS REFERENCE LINK @ {src_uri}: {link}")

        err = check_href(href, src_uri, config)
        if err:
            log.warning(f"{err}: {link}")

    # Cut reference links because they are confused with labels
    markdown = re.sub(LINKS_REFERENCE_RE, "<reference link stripped>", markdown)

    for match in re.finditer(LINKS_LABEL_RE, markdown):
        link = match[0].replace("\n", " ")
        label = match[1].replace("\n", " ")

        used_labels.add(label)
        if label not in reference_links:
            log.warning(f"MISSING REFERENCE LINK @ {src_uri}: {link}")

    for label, link in reference_links.items():
        if label not in used_labels:
            log.warning(f"UNUSED REFERENCE LINK @ {src_uri}: {link}")

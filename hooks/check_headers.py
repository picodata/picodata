import os
import re
from mkdocs.plugins import get_plugin_logger
from mkdocs.config.defaults import MkDocsConfig
from mkdocs.structure.files import Files
from mkdocs.structure.pages import Page
from markdown.extensions.attr_list import AttrListTreeprocessor, get_attrs


log = get_plugin_logger(os.path.basename(__file__))


# https://www.mkdocs.org/dev-guide/plugins/#events
# https://github.com/mkdocs/mkdocs/blob/1.5.3/mkdocs/commands/build.py#L258
def on_files(files: Files, config: MkDocsConfig):
    anchors = dict()
    config["anchors"] = anchors

    for file in files.documentation_pages():
        log.debug(file.src_path)

        Page(None,  file, config)
        assert file.page is not None

        file.page.read_source(config)
        assert file.page.markdown is not None

        markdown: str = file.page.markdown
        lines: list[str] = re.sub("<!--.*?-->", "", markdown, flags=re.DOTALL).split("\n")
        headers: list[str] = filter(lambda line: re.match("^##+ ", line), lines)

        file_anchors = list()
        anchors[file.src_path] = file_anchors

        for line in headers:
            # https://python-markdown.github.io/extensions/attr_list/
            match = AttrListTreeprocessor.HEADER_RE.search(line)
            if not match:
                log.warning(f"MISSING ANCHOR @ {file.src_path}: {line}")
                continue

            attrs = dict(get_attrs(match.group(1)))
            if "id" not in attrs:
                log.warning(f"INVALID ANCHOR @ {file.src_path}: {line}")
                continue

            anchor = attrs["id"]
            if anchor in file_anchors:
                log.warning(f"AMBIGUOUS ANCHOR @ {file.src_path}: {line}")
                continue

            file_anchors.append(anchor)

        # Drop loaded page in order to suppress MkDocs DeprecationWarning:
        # "A plugin has set File.page to an instance of Page and it got overwritten.
        # The behavior of this will change in MkDocs 1.6."
        file.page = None

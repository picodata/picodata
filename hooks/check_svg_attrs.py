import os
import re
from urllib.parse import urlsplit
from mkdocs.plugins import get_plugin_logger
from mkdocs.config.defaults import MkDocsConfig
from mkdocs.structure.files import Files
from mkdocs.structure.pages import Page
from xml.dom import minidom


log = get_plugin_logger(os.path.basename(__file__))


# https://www.mkdocs.org/dev-guide/plugins/#events
def on_files(files: Files, config: MkDocsConfig):
    used_svg: set[str] = set()
    config["used_svg"] = used_svg

    for file in files.documentation_pages():
        Page(None, file, config)
        assert file.page is not None

        file.page.read_source(config)
        assert file.page.markdown is not None

        markdown: str = file.page.markdown
        lines: list[str] = re.sub("<!--.*?-->", "", markdown, flags=re.DOTALL).split("\n")
        images: list[str] = list(filter(lambda line: re.match(r"^!\[", line), lines))

        hrefs: list[str] = re.findall(r"(?<=\()(.*?\.svg)", " ".join(images))

        for href in hrefs:
            dir_path = os.path.dirname(file.abs_src_path)
            svg_path = os.path.normpath(os.path.join(dir_path, href))
            if not os.path.exists(svg_path):
                log.warning(f"MISSING FILE @ {svg_path}")
                continue

            used_svg.add(svg_path)

            dom = minidom.parse(svg_path)

            # Check `viewBox` attribute
            for tag in dom.getElementsByTagName("svg"):
                attr = tag.getAttribute("viewBox")
                width = tag.getAttribute("width").replace("mm", "")
                height = tag.getAttribute("height").replace("mm", "")

                if not attr:
                    log.warning(f"MISSING viewBox @ {svg_path}")
                    continue

                # Ensure values are equal to prevent image distortion
                vals = attr.split()
                if vals[2] != width:
                    log.warning(
                        f'DIFFER width="{width}" AND viewBox=". . {vals[2]} ." @ {svg_path}'
                    )
                if vals[3] != height:
                    log.warning(
                        f'DIFFER height="{height}" AND viewBox=". . . {vals[3]}" @ {svg_path}'
                    )

            # Check `xlink:href` attribute
            for tag in dom.getElementsByTagName("a"):
                attr = tag.getAttribute("xlink:href")
                url = urlsplit(attr)

                if url.path:
                    path = os.path.join(file.src_path, url.path)
                    path = f"{os.path.normpath(path)}.md"
                else:
                    path = file.src_path

                if path not in config["anchors"]:
                    log.warning(f"PAGE NOT FOUND @ {svg_path}: {path}")
                    continue

                if url.fragment not in config["anchors"][path]:
                    log.warning(f"BROKEN ANCHOR @ {svg_path}: #{url.fragment}")
                    continue

        # Drop loaded page in order to suppress MkDocs DeprecationWarning:
        # "A plugin has set File.page to an instance of Page and it got overwritten.
        # The behavior of this will change in MkDocs 1.6."
        file.page = None

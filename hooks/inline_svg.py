import re
import os
from mkdocs.config.defaults import MkDocsConfig
from mkdocs.structure.files import Files
from mkdocs.structure.pages import Page
from xml.dom import minidom
from xml.dom.minidom import Element


def center_svg_texts(elem: Element):
    text_origin = []

    for rect in elem.getElementsByTagName("rect"):
        if not rect.hasAttribute("class"):
            continue

        rect_x = float(rect.getAttribute("x"))
        rect_y = float(rect.getAttribute("y"))
        rect_width = float(rect.getAttribute("width"))
        rect_height = float(rect.getAttribute("height"))

        text_x = rect_x + rect_width / 2
        text_y = rect_y + rect_height / 2

        text_origin.append([str(text_x), str(text_y)])

    for i, text in enumerate(elem.getElementsByTagName("text")):
        text.setAttribute("x", text_origin[i][0])
        text.setAttribute("y", text_origin[i][1])

    return elem


def on_page_markdown(markdown: str, page: Page, config: MkDocsConfig, files: Files):
    markdown = re.sub("<!--.*?-->", "", markdown, flags=re.DOTALL)

    for match in re.finditer(r"( *?)!\[.*?\]\((.*?\.svg)\)", markdown):
        indent, href = match.groups()

        if "/ebnf/" not in href:
            continue

        dir_path = os.path.dirname(page.file.abs_src_path)
        svg_path = os.path.normpath(os.path.join(dir_path, href))

        dom = minidom.parse(svg_path)

        # Cut off XML declaration
        svg_tag = dom.getElementsByTagName("svg")[0]
        assert svg_tag is not None

        svg_tag = center_svg_texts(svg_tag)
        xml = svg_tag.toxml().replace("\n", f"\n{indent}")

        markdown = markdown.replace(match[0].strip(), xml)

    return markdown

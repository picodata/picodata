from bs4 import BeautifulSoup
from bs4.element import Tag
from mkdocs.config.defaults import MkDocsConfig
from urllib.parse import urlsplit
import os
import re


MKDOCS_COMMAND = None
excluded_pages = ["sql_index.md", "connectors_index.md"]


def exclude_pages(items: list, excluded_pages: list) -> list:
    """
    Remove pages from the navigation configuration while preserving
    its structure.

    :param list items: The navigation configuration — the `nav` setting
        from the `mkdocs.yml` file.
    :param list excluded_pages: Pages to exclude.
    :return list: A navigation configuration without excluded pages.
    """
    result: list = []

    for item in items:
        if isinstance(item, str):
            if item not in excluded_pages:
                result.append(item)

        elif isinstance(item, dict):
            new_item: dict = {}
            for k, v in item.items():
                if isinstance(v, str):
                    if v not in excluded_pages:
                        new_item[k] = v
                elif isinstance(v, list):
                    filtered_list = exclude_pages(v, excluded_pages)
                    if filtered_list:
                        new_item[k] = filtered_list
            if new_item:
                result.append(new_item)
    return result


def number_nav_items(items: list, prefix: str = "") -> list[str]:
    """
    Generate a numbered list of items with account to the navigation
    configuration structure.

    :param list items: The navigation configuration.
    :param str prefix: A prefix to generate an item number.
    :return list[str]: A numbered list of items.
    """
    result = []

    for i, item in enumerate(items, 1):

        number = f"{prefix}.{i}" if prefix != "" else str(i)

        if isinstance(item, str):
            result.append(f"{number}. {item}")

        elif isinstance(item, dict):
            for k, v in item.items():
                if isinstance(v, str):
                    pass
                elif isinstance(v, list):
                    result.append(f"{number}. {k}")
                    result.extend(number_nav_items(v, number))
    return result


def update_href(path: str, href: str) -> str:
    """
    Make relative links valid within the PDF document.

    :param str path: The path of the page relative to the source directory
        with the extension omitted.
    :param str href: The URL of the link.
    :return str: An updated URL of the link.
    """
    url = urlsplit(href)

    if url.scheme or url.netloc:
        # Skip external links
        return href

    if not url.path:
        target = path
    else:
        target = os.path.join(path, url.path)
        target = os.path.normpath(target)

    if url.fragment:
        target = os.path.join(target, url.fragment)

    return f"#{target}"


def on_startup(command: str, dirty: bool):
    global MKDOCS_COMMAND
    MKDOCS_COMMAND = command


def on_post_build(config: MkDocsConfig):
    if MKDOCS_COMMAND != "build":
        return

    picodata_doc_ver = os.getenv("PICODATA_DOC_VER", "")

    if picodata_doc_ver == "main":
        picodata_doc_ver = "devel"

    nav = exclude_pages(config["nav"], excluded_pages)
    nav = number_nav_items(nav)

    articles: BeautifulSoup = BeautifulSoup("", "html.parser")

    for item in nav:
        if not item.endswith(".md"):
            h1 = articles.new_tag("h1")
            h1.string = item

            # Insert a page break before the top level `nav` item
            if item.count(".") == 1:
                h1["class"] = ["pd-break-before-page"]

            h1["id"] = item.split()[0].strip(".")

            article = articles.new_tag("article")
            article["class"] = ["md-content__inner", "md-typeset"]
            article.append(h1)

            articles.append(article)
            continue

        prefix, src_uri = item.split()

        path, _ = os.path.splitext(src_uri)
        dest_uri = os.path.join(path, "index.html")
        abs_dest_path = os.path.normpath(os.path.join(config["site_dir"], dest_uri))

        with open(abs_dest_path, "r") as f:
            output = f.read()

        soup = BeautifulSoup(output, "html.parser")
        article = soup.find("article")  # type: ignore

        # Remove the "Edit this page" button ("Исходный код страницы")
        # https://squidfunk.github.io/mkdocs-material/upgrade/#contentaction
        assert isinstance(article.a, Tag)
        article.a.extract()

        # Remove the feedback widget — the only <form> tag on the page
        # https://squidfunk.github.io/mkdocs-material/setup/setting-up-site-analytics/#was-this-page-helpful
        assert isinstance(article.form, Tag)
        article.form.extract()

        # Fix image relative links
        for tag in article.find_all("img"):
            tag["src"] = tag["src"].replace("../", "")

        # Replace collapsible blocks with admonitions
        # See `markdown_extensions` in mkdocs.yml: pymdownx.details, admonition
        for tag in article.find_all("details"):
            tag.name = "div"
            tag["class"].append("admonition")

            admonition_title = tag.summary
            admonition_title.name = "p"
            admonition_title["class"] = ["admonition-title"]

        # Number the contents of <h1> tags and redefine its `id` attribute
        for tag in article.find_all("h1"):
            for child in tag.children:
                if child.name:
                    continue
                title = f"{prefix} {child}"

            tag.string = title
            tag["id"] = path

        # Remove headline anchor links
        # https://squidfunk.github.io/mkdocs-material/setup/extensions/python-markdown/#+toc.permalink
        for tag in article.find_all("a", "headerlink"):
            tag.extract()

        # Update the `id` attribute of <h2>–<h6> tags
        for tag in article.find_all(re.compile(r"^h[2-6]$"), attrs={"id": True}):
            tag["id"] = os.path.join(path, tag["id"])

        # Update relative links
        for tag in article.find_all(attrs={"href": True}):
            tag["href"] = update_href(path, tag["href"])

        # Update relative links in EBNF diagrams
        for tag in article.find_all(attrs={"xlink:href": True}):
            tag["xlink:href"] = update_href(path, tag["xlink:href"])

        # Remove <hr> tags
        for tag in article.find_all("hr"):
            tag.extract()

        # Extract EBNF diagrams from admonitions
        for tag in article.find_all("p", "admonition-title"):
            p = tag.find_next_sibling()
            if p and p.svg:
                tag.extract()
                p.find_parent().unwrap()

        articles.append(article)

    # Generate the table of contents
    ul = articles.new_tag("ul")

    for article in articles.find_all("article"):
        h1 = article.h1  # type: ignore
        a = articles.new_tag("a")
        a.string = h1.string
        a["href"] = f"#{h1["id"]}"

        li = articles.new_tag("li")
        li.append(a)

        ul.append(li)

    h1 = articles.new_tag("h1")
    h1.string = "Содержание"

    toc = articles.new_tag("article")
    toc["class"] = ["md-content__inner md-typeset pd-toc"]
    toc.append(h1)
    toc.append(ul)

    with open("tools/pdf_templates/main.html", "r") as f:
        output = f.read()

    main = BeautifulSoup(output, "html.parser")

    with open("tools/pdf_templates/cover.html", "r") as f:
        output = f.read()

    cover = BeautifulSoup(output, "html.parser")

    assert isinstance(cover.h1, Tag)
    cover.h1.string = f"Picodata {picodata_doc_ver}"

    assert isinstance(main.body, Tag)
    assert isinstance(main.body.div, Tag)
    main.body.div.append(cover)
    main.body.div.append(toc)
    main.body.div.append(articles)

    docs = os.path.join(config["site_dir"], "picodata_docs.html")

    with open(docs, "w") as f:
        f.write(str(main))

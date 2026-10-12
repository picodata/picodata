import os
from mkdocs.config.defaults import MkDocsConfig
from mkdocs.structure.files import Files
from mkdocs.structure.pages import Page


def on_page_markdown(markdown: str, page: Page, config: MkDocsConfig, files: Files):
    # pack-doc in .gitlab-ci.yml writes the documentation version to .env.
    git_ref = os.getenv("PICODATA_DOC_VER") or "master"
    # Documentation built from master is published as devel.
    if git_ref == "devel":
        git_ref = "master"
    return markdown.replace("{{ git_ref }}", git_ref)

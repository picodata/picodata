from mkdocs.config.defaults import MkDocsConfig
from mkdocs.structure.files import Files


def on_files(files: Files, config: MkDocsConfig):
    config["theme"].static_templates.remove("sitemap.xml")

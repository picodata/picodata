import os
from mkdocs.config.defaults import MkDocsConfig


def on_config(config: MkDocsConfig):
    site_url = os.getenv("SITE_URL")
    if site_url:
        config["site_url"] = site_url

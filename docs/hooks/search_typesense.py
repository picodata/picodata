"""Shrink the search index the browser downloads when search goes through Typesense.

Material fetches search/search_index.json on the first search and hands it to the search
worker. With Typesense the worker does not need it, so after the build the full index is
kept as search_index.full.json (the worker loads it only when it falls back to lunr) and
search_index.json keeps just the config. The index is touched only when the built pages
really use search-typesense.js, otherwise the stock lunr worker would get an empty index.
"""
import json
import os

from mkdocs.config.defaults import MkDocsConfig


def on_post_build(config: MkDocsConfig):
    site = config["site_dir"]
    index = os.path.join(site, "search", "search_index.json")
    home = os.path.join(site, "index.html")
    if not os.path.exists(index) or not os.path.exists(home):
        return
    with open(home, encoding="utf-8") as f:
        if "search-typesense.js" not in f.read():
            return

    with open(index, encoding="utf-8") as f:
        full = json.load(f)
    os.replace(index, os.path.join(site, "search", "search_index.full.json"))
    with open(index, "w", encoding="utf-8") as f:
        json.dump({"config": full["config"], "docs": []}, f)

import os
from mkdocs.plugins import get_plugin_logger
from mkdocs.config.defaults import MkDocsConfig
from mkdocs.structure.files import Files


log = get_plugin_logger(os.path.basename(__file__))


def on_files(files: Files, config: MkDocsConfig):
    for file in files.media_files():
        if not file.src_path.startswith("images"):
            continue

        if not file.src_path.endswith(".svg"):
            continue

        if file.src_path not in config["used_svg"]:
            log.info(f"UNUSED SVG: {file.src_path}")

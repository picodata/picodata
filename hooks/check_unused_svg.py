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

        abs_src_path = os.path.join(config["docs_dir"], file.src_path)
        if abs_src_path not in config["used_svg"]:
            log.warning(f"UNUSED SVG: {abs_src_path}")

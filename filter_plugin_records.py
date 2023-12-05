from logging import LogRecord
from mkdocs import utils

def filter_inline_svg(record: LogRecord) -> bool:
    if record.msg.startswith("mkdocs-plugin-inline-svg"):
        return False
    return True

def on_page_markdown(markdown, **kwargs):
    utils.log.addFilter(filter_inline_svg)

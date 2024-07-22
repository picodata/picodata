from mkdocs.config.defaults import MkDocsConfig

MKDOCS_COMMAND = None


def on_startup(command: str, dirty: bool):
    global MKDOCS_COMMAND
    MKDOCS_COMMAND = command


def on_config(config: MkDocsConfig):
    if MKDOCS_COMMAND == "build":
        config.extra["version"] = {"provider": "mike"}

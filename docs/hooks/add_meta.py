from mkdocs.config.defaults import MkDocsConfig
import os


def on_config(config: MkDocsConfig):
    env = dict()
    for k in ["GIT_DESCRIBE", "PICODATA_DOC_VER"]:
        env[k] = os.getenv(k, "")

    config["env"] = env

import logging
import logging.config

LOGGING = {
    "version": 1,
    "loggers": {
        "root": {"level": "INFO"},
    },
}

logging.config.dictConfig(LOGGING)

log = logging.getLogger("root")

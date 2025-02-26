import logging
import logging.config
import sys

LOGGING = {
    "version": 1,
    "loggers": {
        "root": {"level": "INFO"},
    },
}

logging.config.dictConfig(LOGGING)

log = logging.getLogger("root")

handler = logging.StreamHandler(sys.stderr)
handler.setLevel(logging.INFO)
# same as in pytest.ini
formatter = logging.Formatter(
    "%(asctime)s.%(msecs)03d %(levelname)s [%(filename)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
handler.setFormatter(formatter)
log.addHandler(handler)

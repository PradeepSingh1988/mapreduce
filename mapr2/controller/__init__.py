import collections
import logging

import rpyc


rpyc.core.protocol.DEFAULT_CONFIG["allow_pickle"] = True


def exposify(cls):
    for key in dir(cls):
        val = getattr(cls, key)
        if isinstance(val, collections.Callable) and not key.startswith("_"):
            setattr(cls, "exposed_%s" % (key,), val)
    return cls


def setup_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


setup_logger()

import logging

default_format = '%(asctime)s - %(name)s - %(levelname)s: %(message)s'
default_level = logging.DEBUG


def getLogger(name: str, fmt: str=default_format,
              level: str=default_level) -> logging.Logger:
    """
    Wrapper for :func:`logging.getLogger` that applies configuration.

    Parameters
    ----------
    name : str
    fmt : str
    level : int

    Returns
    -------
    :class:`logging.Logger`
    """
    logging.basicConfig(format=default_format)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    return logger

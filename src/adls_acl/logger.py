import logging
from enum import Enum
from sys import stdout


class EnumCallableMixin:
    """Makes enum callable."""

    def __call__(self, *args, **kwargs):
        return self.value(*args, **kwargs)


class HandlerEnum(EnumCallableMixin, Enum):
    stream = logging.StreamHandler
    file = logging.FileHandler


class FormatterStyleEnum(Enum):
    simple = "%(message)s"
    debug = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def configure_logger(
    logger: logging.Logger,
    debug: bool = False,
    silent: bool = False,
    log_file: str = None,
):
    # Determine logging level, formatter, and filters
    filters = []
    if debug:
        logger.setLevel(logging.NOTSET)
        format = logging.Formatter(FormatterStyleEnum.debug.value)
    else:
        logger.setLevel(logging.INFO)
        format = logging.Formatter(FormatterStyleEnum.simple.value)
        filters.append(
            lambda record: "azure" not in record.name
        )  # Filters out logging from azure packages

    # Determine handlers
    handlers = []
    if not silent:
        handlers.append(HandlerEnum.stream(stdout))
    if log_file:
        handlers.append(HandlerEnum.file(filename=log_file, mode="w", encoding="utf-8"))

    # Apply formatters, filters and register handlers
    for handler in handlers:
        handler.setFormatter(format)
        for filter in filters:
            handler.addFilter(filter)

        logger.addHandler(handler)

    return logger

"""Logging helper functions for setting up logging with queue handling and file rotation."""

import logging
import queue
from logging.handlers import QueueHandler, QueueListener, TimedRotatingFileHandler

from rich.console import Console
from rich.logging import RichHandler
from typing_extensions import Literal

from models import LoggingSettings


def listener(
    log_queue: queue.Queue,
    settings: LoggingSettings,
    console: Console,
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO",
) -> QueueListener:
    """
    Create and configure a QueueListener for handling log records from a queue.

    This function sets up a logging listener that processes log records from a queue
    and writes them to a rotating log file. It configures a TimedRotatingFileHandler
    with the specified settings and attaches it to a QueueListener.

    Args:
        log_queue (queue.Queue): The queue from which log records will be consumed.
        settings (LoggingSettings): Configuration object containing logging settings including:
            - log_file_path: Path to the log file
            - log_rotate_time: When to rotate the log file (e.g., 'midnight', 'H', 'D')
            - log_backup_count: Number of backup log files to keep
            - log_format: Format string for log messages
            - log_datetime_format: Format string for datetime in log messages
        console (Console): Rich Console object for pretty logging output.
        level (Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], optional):
            The logging level for the listener. Defaults to "INFO".

    Returns:
        QueueListener: A configured QueueListener instance that will process log records
            from the queue using the configured TimedRotatingFileHandler.

    Example:
        >>> log_queue = queue.Queue()
        >>> settings = LoggingSettings(
        ...     log_file_path='app.log',
        ...     log_rotate_time='midnight',
        ...     log_backup_count=7
        ... )
        >>> queue_listener = listener(log_queue, settings)
        >>> queue_listener.start()
    """
    file_handler = TimedRotatingFileHandler(
        filename=settings.log_file_path,
        when=settings.log_rotate_time,
        backupCount=settings.log_backup_count,
    )
    file_handler.setFormatter(logging.Formatter(fmt=settings.log_format, datefmt=settings.log_datetime_format))

    rich_handler = RichHandler(level=level, console=console, rich_tracebacks=True)
    rich_handler.setFormatter(logging.Formatter(fmt=r"%(message)s", datefmt=settings.log_datetime_format))

    return QueueListener(log_queue, file_handler, rich_handler)


def worker_logger(log_queue: queue.Queue, **kwargs) -> None:
    """
    Configure logging for a worker process using a queue handler.

    This function sets up a logger that sends log records to a queue, which is
    typically used in multiprocessing scenarios where multiple worker processes
    need to send logs to a centralized listener.

    Args:
        log_queue (queue.Queue): A queue object used to send log records from
            the worker process to the main logging process.
        **kwargs: Arbitrary keyword arguments. Supported keys:
            - log_level (str, optional): The logging level for the worker.
                Accepts "DEBUG" or "INFO" (case-insensitive). Defaults to "INFO".

    Returns:
        None

    Example:
        >>> import queue
        >>> import multiprocessing as mp
        >>> log_queue = mp.Queue()
        >>> worker_logger(log_queue, log_level="DEBUG")
    """
    handler = QueueHandler(log_queue)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO if kwargs.get("log_level", "INFO").upper() != "DEBUG" else logging.DEBUG)
    logger.addHandler(handler)

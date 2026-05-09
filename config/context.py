"""Collection of context managers for the project."""

from contextlib import contextmanager
from logging.handlers import QueueListener
from queue import Queue
from typing import Generator, Literal

from influxdb_client.client.influxdb_client import InfluxDBClient
from rich.console import Console

from helper.logging import listener
from models import LoggingSettings


@contextmanager
def logging_context(
    settings: LoggingSettings,
    console: Console,
    log_queue: Queue,
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO",
) -> Generator[QueueListener, None, None]:
    """
    Context manager for setting up and managing a logging system with queue-based handling.

    This context manager initializes a QueueListener for handling log records from a queue,
    starts the listener, yields it for use within the context, and ensures proper cleanup
    by stopping the listener when the context exits.

    Args:
        settings (LoggingSettings): Configuration settings for the logging system.
        console (Console): Console object for output handling (likely from rich library).
        log_queue (Queue, optional): Queue for collecting log records. Defaults to an
            unbounded Queue (maxsize=-1).
        level (Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], optional):
            Logging level threshold. Defaults to "INFO".

    Yields:
        QueueListener: The active queue listener instance that processes log records
            from the queue.

    Example:
        >>> with logging_context(settings, console) as listener:
        ...     # Logging operations happen here
        ...     logger.info("Message will be processed by the listener")
    """
    log_listener: QueueListener = listener(
        log_queue=log_queue,
        settings=settings,
        console=console,
        level=level,
    )
    log_listener.start()
    try:
        yield log_listener
    finally:
        log_listener.stop()


@contextmanager
def influx_conn(conn_vars: dict) -> Generator[InfluxDBClient, None, None]:
    """
    Context manager for creating and closing an InfluxDBClient.

    Args:
        conn_vars: Dictionary of connection parameters passed to InfluxDBClient.

    Yields:
        InfluxDBClient: An active client instance that is closed when the context exits.
    """
    influx_client = InfluxDBClient(**conn_vars)
    try:
        yield influx_client
    finally:
        influx_client.close()

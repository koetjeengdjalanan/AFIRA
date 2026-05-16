"""Collection of context managers for the project."""

from contextlib import contextmanager
from logging import LogRecord
from logging.handlers import QueueListener
from queue import Queue
from typing import Generator

from influxdb_client.client.influxdb_client import InfluxDBClient
from rich.console import Console

from helper.logging import listener
from models import InfluxDBSettings, LoggingSettings, LogLevel


@contextmanager
def logging_context(
    settings: LoggingSettings,
    console: Console,
    log_queue: Queue[LogRecord],
    level: LogLevel = "INFO",
    influxdb_settings: InfluxDBSettings | None = None,
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
        level (LogLevel, optional): Logging level threshold. Defaults to "INFO".
        influxdb_settings (InfluxDBSettings | None, optional): InfluxDB settings used when
            InfluxDB log writing is enabled.

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
        influxdb_settings=influxdb_settings,
    )
    log_listener.start()
    try:
        yield log_listener
    finally:
        log_listener.stop()
        for handler in log_listener.handlers:
            handler.close()


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

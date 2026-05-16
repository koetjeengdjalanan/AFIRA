"""Logging helper functions for setting up logging with queue handling and file rotation."""

import datetime
import logging
import queue
import re
import threading
from logging.handlers import QueueHandler, QueueListener, TimedRotatingFileHandler
from typing import Protocol

from influxdb_client.client.influxdb_client import InfluxDBClient
from influxdb_client.client.write.point import Point
from influxdb_client.client.write_api import SYNCHRONOUS, WriteApi
from rich.console import Console
from rich.logging import RichHandler

from models import InfluxDBSettings, LoggingSettings, LogLevel

_LOG_LEVEL_VALUES: dict[LogLevel, int] = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}
_SECRET_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(r"(?i)(['\"]?access_token['\"]?\s*[:=]\s*['\"])[^'\"]+(['\"])"),
        r"\1***\2",
    ),
    (
        re.compile(r"(?i)(['\"]?client_secret['\"]?\s*[:=]\s*['\"])[^'\"]+(['\"])"),
        r"\1***\2",
    ),
    (
        re.compile(r"(?i)(Authorization['\"]?\s*[:=]\s*['\"]?Bearer\s+)[^'\"\s,}]+"),
        r"\1***",
    ),
    (
        re.compile(r"(?i)(Bearer\s+)[A-Za-z0-9._~+/=-]+"),
        r"\1***",
    ),
    (
        re.compile(r"(?i)(Token\s+)[A-Za-z0-9._~+/=-]+"),
        r"\1***",
    ),
)


def _redact_log_message(message: str) -> str:
    """Return a log message with common secret values redacted."""
    redacted_message = message
    for pattern, replacement in _SECRET_PATTERNS:
        redacted_message = pattern.sub(replacement, redacted_message)
    return redacted_message


class SecretRedactionFilter(logging.Filter):
    """Logging filter that redacts common secret values from log messages."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Redact the current record message before handlers write it."""
        record.msg = _redact_log_message(record.getMessage())
        record.args = ()
        return True


class InfluxLogWriter(Protocol):
    """Protocol for InfluxDB write APIs used by log writing helpers."""

    def write(self, bucket: str, org: str, record: Point) -> None:
        """Write one log point to InfluxDB."""


class _WriteApiLogWriter:
    """Adapter that gives the InfluxDB WriteApi a narrow log-writer interface."""

    def __init__(self, write_api: WriteApi) -> None:
        """Initialize the adapter with an InfluxDB write API."""
        self._write_api = write_api

    def write(self, bucket: str, org: str, record: Point) -> None:
        """Write one log point through the InfluxDB write API."""
        _ = self._write_api.write(bucket=bucket, org=org, record=record)


def _log_level_value(level: LogLevel) -> int:
    """Return a stdlib logging level value for a typed log level."""
    return _LOG_LEVEL_VALUES[level]


def _build_influx_log_point(record: logging.LogRecord, measurement: str) -> Point:
    """Build an InfluxDB point from a logging record."""
    process_id: int = record.process if record.process is not None else 0
    message = _redact_log_message(record.getMessage())

    point = (
        Point(measurement)
        .tag("logger", record.name)
        .tag("level", record.levelname)
        .tag("module", record.module)
        .field("message", message)
        .field("path", record.pathname)
        .field("line", int(record.lineno))
        .field("function", record.funcName)
        .field("process", process_id)
        .field("thread", record.threadName)
        .time(datetime.datetime.fromtimestamp(record.created, tz=datetime.UTC))
    )

    if record.exc_info is not None:
        point.field("exception", logging.Formatter().formatException(record.exc_info))
    if record.stack_info is not None:
        point.field("stack_info", record.stack_info)

    return point


def _validate_influxdb_logging_settings(influx_conf: InfluxDBSettings) -> None:
    """Validate the InfluxDB settings required for log writes."""
    missing_fields: list[str] = [
        field_name
        for field_name, value in (
            ("url", influx_conf.url),
            ("token", influx_conf.token),
            ("org", influx_conf.org),
            ("bucket", influx_conf.bucket),
        )
        if not value
    ]
    if missing_fields:
        missing = ", ".join(missing_fields)
        raise ValueError(f"InfluxDB logging is enabled but missing required settings: {missing}.")


def write_log_record_to_influxdb(
    record: logging.LogRecord,
    writer: InfluxLogWriter,
    influx_conf: InfluxDBSettings,
    measurement: str,
) -> None:
    """Write one log record to InfluxDB."""
    writer.write(
        bucket=influx_conf.bucket,
        org=influx_conf.org,
        record=_build_influx_log_point(record=record, measurement=measurement),
    )


class InfluxDBLogHandler(logging.Handler):
    """Logging handler that writes records into InfluxDB."""

    def __init__(
        self,
        influx_conf: InfluxDBSettings,
        measurement: str,
        level: LogLevel,
    ) -> None:
        """Initialize an InfluxDB-backed logging handler."""
        super().__init__(level=_log_level_value(level))
        self._influx_conf = influx_conf
        self._measurement = measurement
        self._client: InfluxDBClient | None = None
        self._write_api: WriteApi | None = None
        self._lock = threading.RLock()
        self._is_emitting = False

    def _ensure_write_api(self) -> WriteApi:
        """Return an initialized InfluxDB write API."""
        if self._client is None:
            self._client = InfluxDBClient(
                url=self._influx_conf.url,
                token=self._influx_conf.token,
                org=self._influx_conf.org,
                timeout=int(self._influx_conf.timeout_ms),
                enable_gzip=True,
                connection_pool_maxsize=int(self._influx_conf.connection_pool_maxsize),
            )
            self._write_api = self._client.write_api(write_options=SYNCHRONOUS)
        if self._write_api is None:
            raise RuntimeError("InfluxDB write API was not initialized.")
        return self._write_api

    def emit(self, record: logging.LogRecord) -> None:
        """Write a log record to InfluxDB."""
        if self._is_emitting:
            return

        self._is_emitting = True
        try:
            with self._lock:
                write_log_record_to_influxdb(
                    record=record,
                    writer=_WriteApiLogWriter(write_api=self._ensure_write_api()),
                    influx_conf=self._influx_conf,
                    measurement=self._measurement,
                )
        except Exception:
            self.handleError(record)
        finally:
            self._is_emitting = False

    def close(self) -> None:
        """Close the InfluxDB client resources."""
        try:
            if self._write_api is not None:
                self._write_api.close()
            if self._client is not None:
                self._client.close()
        finally:
            self._write_api = None
            self._client = None
            super().close()


def listener(
    log_queue: queue.Queue[logging.LogRecord],
    settings: LoggingSettings,
    console: Console,
    level: LogLevel = "INFO",
    influxdb_settings: InfluxDBSettings | None = None,
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
        level (LogLevel, optional): The logging level for the listener. Defaults to "INFO".
        influxdb_settings (InfluxDBSettings | None, optional): InfluxDB connection settings used
            when InfluxDB log writing is enabled.

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
    settings.log_file_path.parent.mkdir(parents=True, exist_ok=True)
    settings.log_file_path.touch(exist_ok=True)

    file_handler = TimedRotatingFileHandler(
        filename=str(settings.log_file_path),
        when=settings.log_rotate_time,
        backupCount=settings.log_backup_count,
    )
    file_handler.addFilter(SecretRedactionFilter())
    file_handler.setLevel(_log_level_value(level))
    file_handler.setFormatter(logging.Formatter(fmt=settings.log_format, datefmt=settings.log_datetime_format))

    rich_handler = RichHandler(level=_log_level_value(level), console=console, rich_tracebacks=True)
    rich_handler.addFilter(SecretRedactionFilter())
    rich_handler.setFormatter(logging.Formatter(fmt=r"%(message)s", datefmt=settings.log_datetime_format))

    handlers: list[logging.Handler] = [file_handler, rich_handler]

    if settings.log_influxdb_enabled:
        if influxdb_settings is None:
            raise ValueError("InfluxDB logging is enabled but no InfluxDB settings were provided.")
        _validate_influxdb_logging_settings(influxdb_settings)
        influxdb_handler = InfluxDBLogHandler(
            influx_conf=influxdb_settings,
            measurement=settings.log_influxdb_measurement,
            level=settings.log_influxdb_level,
        )
        influxdb_handler.addFilter(SecretRedactionFilter())
        handlers.append(influxdb_handler)

    return QueueListener(log_queue, *handlers, respect_handler_level=True)


def worker_logger(log_queue: queue.Queue[logging.LogRecord], log_level: LogLevel = "INFO") -> None:
    """
    Configure logging for a worker process using a queue handler.

    This function sets up a logger that sends log records to a queue, which is
    typically used in multiprocessing scenarios where multiple worker processes
    need to send logs to a centralized listener.

    Args:
        log_queue (queue.Queue): A queue object used to send log records from
            the worker process to the main logging process.
        log_level (LogLevel, optional): Minimum log level sent to the logging queue.
            Defaults to "INFO".

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
    logger.setLevel(_log_level_value(log_level))
    for existing_handler in list(logger.handlers):
        if isinstance(existing_handler, QueueHandler):
            logger.removeHandler(existing_handler)
    logger.addHandler(handler)

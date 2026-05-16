"""Tests for AFIRA logging helpers."""

import io
import logging
import queue
from pathlib import Path

from influxdb_client.client.write.point import Point
from rich.console import Console

from config.context import logging_context
from helper.logging import worker_logger, write_log_record_to_influxdb
from models import InfluxDBSettings, LoggingSettings


class FakeInfluxLogWriter:
    """Fake InfluxDB writer for log point assertions."""

    def __init__(self) -> None:
        """Initialize captured write state."""
        self.bucket: str | None = None
        self.org: str | None = None
        self.record: Point | None = None

    def write(self, bucket: str, org: str, record: Point) -> None:
        """Capture an InfluxDB point write."""
        self.bucket = bucket
        self.org = org
        self.record = record


def test_logging_context_writes_log_records_to_file(tmp_path: Path) -> None:
    """Queue logging should write records into the configured file path."""
    log_file = tmp_path / "afira.log"
    settings = LoggingSettings(log_file_path=log_file)
    console = Console(file=io.StringIO(), force_terminal=False, width=120)
    log_queue: queue.Queue[logging.LogRecord] = queue.Queue(maxsize=-1)
    root_logger = logging.getLogger()
    previous_handlers = list(root_logger.handlers)
    previous_level = root_logger.level

    for handler in previous_handlers:
        root_logger.removeHandler(handler)

    try:
        with logging_context(settings=settings, console=console, log_queue=log_queue, level="DEBUG"):
            worker_logger(log_queue=log_queue, log_level="DEBUG")
            logging.getLogger("AFIRA.Test").debug("file logger is alive")
    finally:
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)
        for handler in previous_handlers:
            root_logger.addHandler(handler)
        root_logger.setLevel(previous_level)

    assert "file logger is alive" in log_file.read_text(encoding="utf-8")


def test_write_log_record_to_influxdb_uses_configured_destination() -> None:
    """InfluxDB log writes should use the configured bucket, org, and measurement."""
    writer = FakeInfluxLogWriter()
    influx_settings = InfluxDBSettings(
        token="token",
        url="http://localhost:8086",
        org="afira-org",
        bucket="afira-bucket",
    )
    record = logging.LogRecord(
        name="AFIRA.Test",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="influx logger is alive",
        args=(),
        exc_info=None,
    )

    write_log_record_to_influxdb(
        record=record,
        writer=writer,
        influx_conf=influx_settings,
        measurement="afira_logs",
    )

    assert writer.bucket == "afira-bucket"
    assert writer.org == "afira-org"
    assert writer.record is not None
    line_protocol = writer.record.to_line_protocol()
    assert line_protocol.startswith("afira_logs,")
    assert "level=ERROR" in line_protocol
    assert 'message="influx logger is alive"' in line_protocol


def test_write_log_record_to_influxdb_redacts_token_values() -> None:
    """InfluxDB log writes should redact common token values from messages."""
    writer = FakeInfluxLogWriter()
    influx_settings = InfluxDBSettings(
        token="token",
        url="http://localhost:8086",
        org="afira-org",
        bucket="afira-bucket",
    )
    record = logging.LogRecord(
        name="AFIRA.Test",
        level=logging.DEBUG,
        pathname=__file__,
        lineno=1,
        msg="Authorization: Bearer super-secret-token access_token': 'another-secret'",
        args=(),
        exc_info=None,
    )

    write_log_record_to_influxdb(
        record=record,
        writer=writer,
        influx_conf=influx_settings,
        measurement="afira_logs",
    )

    assert writer.record is not None
    line_protocol = writer.record.to_line_protocol()
    assert "super-secret-token" not in line_protocol
    assert "another-secret" not in line_protocol
    assert "Bearer ***" in line_protocol

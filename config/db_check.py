"""Ensure that the database is properly configured and accessible."""

import logging

from config.context import influx_conn
from models import InfluxDBSettings


def test_db_setup(setting: InfluxDBSettings) -> None:
    """
    Verify that the configured InfluxDB instance is reachable.

    Opens an InfluxDB client with the connection parameters from the provided
    settings, pings the server until it responds or the configured retry limit
    is reached, and logs the available organizations after a successful
    connection.

    Args:
        setting (InfluxDBSettings): InfluxDB connection settings, including
            connection parameters and the maximum number of ping retries.

    Raises:
        ConnectionError: If InfluxDB does not respond successfully within the
            configured retry limit.
    """
    logger = logging.getLogger("AFIRA.DBCheck")
    iterate: int = 0
    with influx_conn(conn_vars=setting.conn_params()) as client:
        while iterate < setting.max_retries:
            ping_res = client.ping()
            if ping_res:
                break
            iterate += 1
        if not ping_res:
            raise ConnectionError("Unable to connect to InfluxDB. Please check your connection settings.")
        influx_org = client.organizations_api().find_organizations()
        logger.debug(f"Successfully connected to InfluxDB. Available organizations: {[org.name for org in influx_org]}")
        bucket = client.buckets_api().find_bucket_by_name(setting.bucket)
        if any(
            [
                bucket is None,
                bucket == [],
                bucket == {},
            ]
        ):
            raise ConnectionError(
                f"Bucket '{setting.bucket}' not found in InfluxDB. Please check your bucket settings.",
            )

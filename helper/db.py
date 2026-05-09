"""Database connection utility functions."""

import logging
from os import getenv
from pathlib import Path

import requests
import yaml
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client.client.write.point import Point

from models import InfluxDBSettings


def retrieve_creds() -> dict[str, dict[str, str]]:
    """
    Load database credentials from a YAML configuration file.

    Reads the 'creds.yaml' file and parses its contents using YAML format.

    Returns:
        dict[str, dict[str, str]]: A dictionary containing the parsed credentials from the YAML file.
    """
    creds_path: Path = Path(Path.cwd() / "creds.yaml").absolute()
    with open(file=creds_path, mode="r") as creds:
        return yaml.safe_load(creds)


def token_test(token: str) -> bool:
    """Test Provided Origin Token If exist.

    Args:
        token (str): Token String.

    Returns:
        bool: Either exists `True` or nonexistent `False`.
    """
    headers: dict[str, str] = {"Authorization": f"Token {token}"}
    INFLUXDB_URL: str = getenv("INFLUXDB_URL", "http://influxdb:8086")
    try:
        res = requests.get(url=f"{INFLUXDB_URL}/api/v2/orgs", headers=headers, timeout=15)
        if res.status_code == 200:
            print("Token test successful")
            return True
        else:
            print(f"Token test failed: {res.status_code} - {res.text}")
            return False
    except Exception as e:
        print(f"Token test error: {e}")
        return False


def get_token():
    """Get or Generate an InfluxDB token."""
    ORIGIN_TOKEN: str = getenv("DOCKER_INFLUXDB_INIT_ADMIN_TOKEN", "")
    try:
        if ORIGIN_TOKEN != "":
            print("Using InfluxDB token from environment variable")
            if token_test(ORIGIN_TOKEN):
                return ORIGIN_TOKEN
            else:
                raise ValueError(f"Provided token from environment variable is invalid! - {ORIGIN_TOKEN}")
    except Exception as e:
        print(f"Failed to get InfluxDB token from environment variable: {e}")
        raise


async def store_points(points: list[Point], influx_conf: InfluxDBSettings, debug_mode: bool = False) -> None:
    """
    Asynchronously stores a list of points to an InfluxDB bucket with retry logic.

    This function attempts to write the provided points to the specified InfluxDB instance
    using the given configuration. It handles retries in case of failures, up to the maximum
    number specified in the configuration. Points are written in batches to optimize performance.

    Args:
        points (list[Point]): A list of Point objects to be stored in InfluxDB.
        influx_conf (InfluxDBSettings): Configuration object containing InfluxDB connection details,
            such as URL, token, bucket, org, timeout, max_retries, and batch_size.
        debug_mode (bool, optional): If True, enables debug mode for the InfluxDB client.
            Defaults to False.

    Raises:
        Exception: If all retry attempts fail, the last encountered exception is raised.

    Returns:
        None
    """
    log = logging.getLogger("AFIRA.DBStore")
    if not points or len(points) == 0:
        return

    for attemp in range(1, influx_conf.max_retries + 1):
        try:
            async with InfluxDBClientAsync(
                url=influx_conf.url, token=influx_conf.token, debug=debug_mode, timeout=influx_conf.timeout_ms
            ) as client:
                write_api = client.write_api()
                for i in range(0, len(points), influx_conf.batch_size):
                    batch = points[i : i + influx_conf.batch_size]
                    await write_api.write(bucket=influx_conf.bucket, org=influx_conf.org, record=batch)
        except Exception as e:
            if attemp == influx_conf.max_retries:
                raise e
            log.warning(f"Attempt {attemp} failed to store points in InfluxDB. Retrying...", e)
    return


if __name__ == "__main__":
    res = retrieve_creds()
    print(res)
    print("done")

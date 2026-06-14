"""Fortigate log fetcher for disk event system logs."""

import time
from typing import Any

from influxdb_client.client.write.point import Point
from influxdb_client.domain.write_precision import WritePrecision

from models import FortigateClient


def log_disk_event_system(
    api_client: FortigateClient,
    serial_no: str,
    vdoms: list[str],
    interval_s: int = 300,
) -> tuple[list[dict[str, Any]], list[Point]]:
    """
    Fetch critical system events from the disk log for the given HA member and VDOMs.

    Args:
        api_client: An instance of FortigateClient to make API calls.
        serial_no: The serial number of the HA member to fetch logs for.
        vdoms: A list of VDOM names to fetch logs from.
        interval_s: The time interval in seconds to look back for logs (default is 300 seconds).

    Returns:
        tuple[list[dict[str, Any]], list[Point]]: A tuple containing a list of log entries
            as dictionaries and a list of InfluxDB Point objects for the logs.
    """
    log_items: list[dict[str, Any]] = []
    points: list[Point] = []

    now_ms: int = int(time.time() * 1000)
    start_time_ms: int = now_ms - (interval_s * 1000)

    for vdom in vdoms:
        start = 0
        rows = 500

        while True:
            params: list[tuple[str, Any]] = [
                ("serial_no", serial_no),
                ("vdom", vdom),
                ("start", start),
                ("rows", rows),
                ("filter", 'subtype=*"system"'),
                ("filter", 'level="critical"'),
                ("filter", f"_metadata.timestamp>={start_time_ms}"),
                ("filter", f"_metadata.timestamp<={now_ms}"),
                ("extra", "country_id"),
                ("extra", "reverse_lookup"),
            ]
            res = api_client.get(
                "/api/v2/log/disk/event/system",
                params=params,
                verify=False,
            )
            res_json = res.json()

            if not res.ok or "results" not in res_json:
                raise Exception(
                    f"Failed to fetch log disk event system for vdom '{vdom}': {res.text}"
                )

            results: list[dict[str, Any]] = res_json.get("results", [])
            total_lines: int = res_json.get("total_lines", 0)
            response_serial: str = res_json.get("serial", serial_no)

            for entry in results:
                metadata: dict[str, Any] = entry.get("_metadata", {})
                timestamp_ms: int = metadata.get("timestamp", 0)
                status: str = entry.get("status", "unknown")
                entry_vdom: str = entry.get("vd", vdom)
                level: str = entry.get("level", "unknown")
                name: str = entry.get("name", "")
                interface: str = entry.get("interface", "")
                logdesc: str = entry.get("logdesc", "")
                logid: str = entry.get("logid", "")

                log_items.append({
                    "serial": response_serial,
                    "vdom": entry_vdom,
                    "level": level,
                    "name": name,
                    "interface": interface,
                    "status": status,
                    "logdesc": logdesc,
                    "logid": logid,
                    "msg": entry.get("msg", ""),
                    "timestamp": timestamp_ms,
                })

                point = (
                    Point("fortigate_log_event_system")
                    .tag("serial_no", response_serial)
                    .tag("vdom", entry_vdom)
                    .tag("level", level)
                    .tag("name", name)
                    .tag("interface", interface)
                    .tag("status", status)
                    .tag("logdesc", logdesc)
                    .tag("logid", logid)
                    .field("status_value", 1 if status == "up" else 0)
                    .field("msg", entry.get("msg", ""))
                    .time(timestamp_ms, WritePrecision.MS)
                )
                points.append(point)

            start += rows
            if start >= total_lines or not results:
                break

    return log_items, points

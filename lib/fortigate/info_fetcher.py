"""Fortigate info fetchers: HA checksum, firmware, log device state, and CSF."""

from datetime import datetime, timedelta
from typing import Any

from influxdb_client.client.write.point import Point

from models import FortigateClient


def ha_checksum(api_client: FortigateClient) -> tuple[list[dict[str, list[str]]], list[Point]]:
    """
    Fetches HA checksum information from the Fortigate API.

    Args:
        api_client (FortigateClient): An instance of the FortigateClient to interact with the API.

    Returns:
        tuple[list[dict[str, list[str]]], list[Point]]: A tuple containing:
            - A list of dicts with {"serial_no": ..., "vdoms": ...} for each HA member.
            - A list of InfluxDB Points indicating sync/desync state per HA member.
    """
    res = api_client.get(
        "/api/v2/monitor/system/ha-checksums",
    )
    res_json = res.json()

    if not res.ok or "results" not in res_json:
        raise Exception(f"Failed to fetch HA checksum information: {res.text}")

    results = res_json.get("results", [])

    # Use the primary's checksum as the source of truth
    primary_checksum = next(
        (item.get("checksum", {}).get("all", "") for item in results if item.get("is_manage_primary", False)),
        ""
    )

    ha_members: list[dict[str, list[str]]] = []
    points: list[Point] = []

    for item in results:
        serial_no = item.get("serial_no", "unknown")
        is_primary = item.get("is_manage_primary", False)
        member_checksum = item.get("checksum", {}).get("all", "")
        vdoms = (
            item.get("checksum", {}).get("vdoms", {}).keys()
            if "checksum" in item and "vdoms" in item["checksum"]
            else []
        )

        # Primary is always considered synced; secondary is synced if its checksum matches primary
        is_synced = is_primary or (member_checksum == primary_checksum and primary_checksum != "")

        ha_members.append({
            "serial_no": serial_no,
            "vdoms": list(vdoms),
        })

        point = (
            Point("ha_sync_state")
            .tag("serial_no", serial_no)
            .tag("role", "primary" if is_primary else "secondary")
            .field("is_synced", 1 if is_synced else 0)
        )
        points.append(point)

    return ha_members, points


def firmware(api_client: FortigateClient) -> tuple[dict[str, Any], list[Point]]:
    """
    Fetches firmware information from the Fortigate API.

    Returns:
        tuple[dict[str, Any], list[Point]]: A tuple containing:
            - A dictionary with firmware information.
            - A list of InfluxDB Points indicating current firmware version and update availability.
    """
    res = api_client.get("/api/v2/monitor/system/firmware")
    res_json = res.json()

    if not res.ok or "results" not in res_json:
        raise Exception(f"Failed to fetch firmware information: {res.text}")

    # Update available matrics
    firmware_info: dict[str, Any] = res_json.get("results", {})
    current_firmware: dict[str, Any] = firmware_info.get("current", {})
    available_firmware: list[dict[str, Any]] = firmware_info.get("available", [])

    last_new_version: dict[str, Any] = available_firmware[0] if available_firmware else {}
    points: list[Point] = []

    update_history_firmware_point = (
        Point("firmware_update_history")
        .tag("serial_no", firmware_info.get("serial_no", "unknown"))
        .field("current_version", current_firmware.get("version", "unknown"))
    )
    points.append(update_history_firmware_point)

    if current_firmware and last_new_version:
        firmware_update_available_point = (
            Point("firmware_update_available")
            .tag("serial_no", firmware_info.get("serial_no", "unknown"))
            .tag("current_version", current_firmware.get("version", "unknown"))
            .tag("available_version", last_new_version.get("version", "unknown"))
            .field(
                "is_update_available",
                1 if current_firmware.get("version") != last_new_version.get("version") else 0,
            )
            .field("release_notes", last_new_version.get("release_notes", ""))
        )
        points.append(firmware_update_available_point)

    return firmware_info, points


def log_device_state(api_client: FortigateClient) -> tuple[dict[str, str|bool|dict[str, int|bool]], list[Point]]:
    """
    Fetches log device state information from the Fortigate API.

    Args:
        api_client (FortigateClient): An instance of the FortigateClient to interact with the API.

    Returns:
        tuple[dict[str, str|bool|dict[str, int|bool]], list[Point]]: A tuple containing:
            - A dictionary with log device state information.
            - A list of InfluxDB Points indicating log device states.
    """
    res = api_client.get("/api/v2/monitor/log/device/state")
    res_json = res.json()

    if not res.ok or "results" not in res_json:
        raise Exception(f"Failed to fetch log device state information: {res.text}")

    results: dict[str, Any] = res_json.get("results", {})
    serial_no: str = res_json.get("serial", "unknown")
    vdom: str = res_json.get("vdom", "unknown")
    points: list[Point] = []

    # One point per named log device (memory, disk, fortianalyzer, fortianalyzer_cloud, forticloud)
    device_keys = ("memory", "disk", "fortianalyzer", "fortianalyzer_cloud", "forticloud")
    for device in device_keys:
        device_info = results.get(device)
        if not isinstance(device_info, dict):
            continue

        point = (
            Point("fortigate_log_device_state")
            .tag("serial_no", serial_no)
            .tag("vdom", vdom)
            .tag("device", device)
            .field("is_available", 1 if device_info.get("is_available", False) else 0)
            .field("is_enabled", 1 if device_info.get("is_enabled", False) else 0)
            .field("is_ha_supported", 1 if device_info.get("is_ha_supported", False) else 0)
        )

        # disk-specific fields
        if device == "disk":
            point = (
                point
                .field("is_loggable", 1 if device_info.get("is_loggable", False) else 0)
                .field("num_ssds_available", int(device_info.get("num_ssds_available", 0)))
                .field("disabled_by_default", 1 if device_info.get("disabled_by_default", False) else 0)
                .field("is_fortiview_enabled", 1 if device_info.get("is_fortiview_enabled", False) else 0)
                .field("fortiview_weekly_data", 1 if device_info.get("fortiview_weekly_data", False) else 0)
            )

        # memory-specific field
        if device == "memory":
            point = point.field("is_default", 1 if device_info.get("is_default", False) else 0)

        # fortianalyzer-specific field
        if device == "fortianalyzer":
            point = point.field("overrides_global_faz", 1 if device_info.get("overrides_global_faz", False) else 0)

        # fortianalyzer_cloud-specific field
        if device == "fortianalyzer_cloud":
            point = point.field(
                "overrides_global_faz_cloud",
                1 if device_info.get("overrides_global_faz_cloud", False) else 0,
            )

        # forticloud-specific field
        if device == "forticloud":
            point = point.field("is_faz_cloud", 1 if device_info.get("is_faz_cloud", False) else 0)

        points.append(point)

    # One summary point for global state
    summary_point = (
        Point("fortigate_log_device_summary")
        .tag("serial_no", serial_no)
        .tag("vdom", vdom)
        .tag("log_device_default", results.get("log_device_default", "unknown"))
        .field("is_fortiview_supported", 1 if results.get("is_fortiview_supported", False) else 0)
    )
    points.append(summary_point)

    return results, points


def cooperative_security_fabric(api_client: FortigateClient) -> tuple[dict[str, Any], list[Point]]:
    """
    Fetch Cooperative Security Fabric (CSF) information and convert it to InfluxDB Points.

    Args:
        api_client (FortigateClient): An instance of FortigateClient used to reach the FortiOS REST API.

    Returns:
        tuple[dict[str, Any], list[Point]]: Raw parsed results dict and a list of InfluxDB Points
            covering device info, state, HA, VDOM counts/features, and the global CSF protocol status.

    Raises:
        Exception: If the HTTP call fails or the response is missing the ``results`` key.
    """
    res = api_client.get(
        "/api/v2/monitor/system/csf",
        params={"vdom": "root", "scope": "vdom", "decode": "true"},
    )
    res_json: dict[str, Any] = res.json()

    if not res.ok or "results" not in res_json:
        raise Exception(f"Failed to fetch CSF information: {res.text}")

    results: dict[str, Any] = res_json["results"]
    devices_section: dict[str, Any] = results.get("devices", {})
    fortigate_devices: list[dict[str, Any]] = devices_section.get("fortigate", [])
    protocol_enabled: bool = results.get("protocol_enabled", False)

    csf_data: dict[str, Any] = {
        "protocol_enabled": protocol_enabled,
        "devices": fortigate_devices,
    }

    points: list[Point] = []

    for device in fortigate_devices:
        if not isinstance(device, dict):
            continue

        points.extend(_device_info_point(device))
        points.extend(_device_state_point(device))

    return csf_data, points


def _device_info_point(device: dict[str, Any]) -> list[Point]:
    """
    Build a ``fortigate_csf_device_info`` Point with static identity and firmware version.

    Tags: serial_no, hostname, model_name, model_number, device_type.
    Fields: fw_major, fw_minor, fw_patch, fw_build.

    Args:
        device (dict[str, Any]): A dictionary containing device information from the CSF API response.

    Returns:
        list[Point]: A list containing a single InfluxDB Point with device information.
    """
    serial = device.get("serial", "unknown")
    hostname = device.get("host_name", "unknown")
    model_name = device.get("model_name", "unknown")
    model_num = device.get("model_number", "unknown")
    device_type = device.get("device_type", "unknown")

    fw_major = device.get("firmware_version_major", 0)
    fw_minor = device.get("firmware_version_minor", 0)
    fw_patch = device.get("firmware_version_patch", 0)
    fw_build = device.get("firmware_version_build", 0)

    point = (
        Point("fortigate_csf_device_info")
        # --- tags (low-cardinality, used for filtering / grouping) ---
        .tag("serial_no", serial)
        .tag("hostname", hostname)
        .tag("model_name", model_name)
        .tag("model_number", model_num)
        .tag("device_type", device_type)
        # --- fields (numeric/boolean values to plot) ---
        .field("firmware_version_major", int(fw_major))
        .field("firmware_version_minor", int(fw_minor))
        .field("firmware_version_patch", int(fw_patch))
        .field("firmware_version_build", int(fw_build))
    )
    return [point]


def _device_state_point(device: dict[str, Any]) -> list[Point]:
    """Build a ``fortigate_csf_device_uptime`` Point from the device state.

    Args:
        device (dict[str, Any]): A dictionary containing device information from the CSF API response.

    Returns:
        list[Point]: A list containing a single InfluxDB Point with device uptime information.
    """
    serial = device.get("serial", "unknown")

    state: dict[str, Any] = device.get("state", {})

    current_timestamp_unix = datetime.now().timestamp() * 1000
    last_reboot = state.get("utc_last_reboot", 0)
    uptime_ms = max(current_timestamp_unix - last_reboot, 0)

    duration = timedelta(milliseconds=uptime_ms)
    total_seconds = int(duration.total_seconds())

    days = total_seconds // 86400
    hours = (total_seconds % 86400) // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    uptime_text = f"{days}d {hours}h {minutes}m {seconds}s"

    point = (
        Point("fortigate_csf_device_uptime")
        .tag("serial_no", serial)
        .field("uptime_ms", int(uptime_ms))
        .field("uptime_text", uptime_text)
    )
    return [point]

def interface_monitor(
    api_client: FortigateClient,
    vdom: str,
) -> tuple[list[dict[str, Any]], list[Point]]:
    """
    Fetch per-interface live statistics from the Fortigate monitor API for a VDOM.

    The endpoint response already contains the interface records for the requested VDOM,
    so this function uses that payload directly without relying on any other interface
    configuration helper.

    Args:
        api_client (FortigateClient): An authenticated Fortigate API client.
        vdom (str): The VDOM name to query.

    Returns:
        tuple[list[dict[str, Any]], list[Point]]: A tuple of:
            - A list of interface dicts built from the monitor payload.
            - A list of InfluxDB Points, one per interface in the monitor response.

    Raises:
        Exception: If the HTTP request fails or the response body is missing ``results``.
    """
    res = api_client.get(
        "/api/v2/monitor/system/interface",
        params={"vdom": vdom}
    )
    res_json: dict[str, Any] = res.json()

    if not res.ok or "results" not in res_json:
        raise Exception(f"Failed to fetch interface monitor data for VDOM {vdom}: {res.text}")

    monitor_results: dict[str, dict[str, Any]] = res_json.get("results", {})
    serial_no: str = res_json.get("serial", "unknown")
    response_vdom: str = res_json.get("vdom", vdom)

    enriched: list[dict[str, Any]] = []
    points: list[Point] = []

    for interface_name, monitor_data in monitor_results.items():
        if not isinstance(monitor_data, dict):
            continue

        name: str = monitor_data.get("name", interface_name)
        alias: str = monitor_data.get("alias", "")

        merged: dict[str, Any] = dict(monitor_data)
        merged.setdefault("vdom", response_vdom)
        merged.setdefault("name", name)
        merged.setdefault("alias", alias)
        enriched.append(merged)

        link: bool = bool(monitor_data.get("link", False))
        speed: float = float(monitor_data.get("speed", 0.0))
        duplex: int = int(monitor_data.get("duplex", 0))
        tx_packets: int = int(monitor_data.get("tx_packets", 0))
        rx_packets: int = int(monitor_data.get("rx_packets", 0))
        tx_bytes: int = int(monitor_data.get("tx_bytes", 0))
        rx_bytes: int = int(monitor_data.get("rx_bytes", 0))
        tx_errors: int = int(monitor_data.get("tx_errors", 0))
        rx_errors: int = int(monitor_data.get("rx_errors", 0))

        point = (
            Point("fortigate_interface_monitor")
            .tag("serial_no", serial_no)
            .tag("vdom", response_vdom)
            .tag("interface_name", name)
            .tag("alias", alias)
            .field("link", 1 if link else 0)
            .field("speed", speed)
            .field("duplex", duplex)
            .field("tx_packets", tx_packets)
            .field("rx_packets", rx_packets)
            .field("tx_bytes", tx_bytes)
            .field("rx_bytes", rx_bytes)
            .field("tx_errors", tx_errors)
            .field("rx_errors", rx_errors)
        )
        points.append(point)

    return enriched, points

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
        verify=False # Disable SSL verification for self-signed certificates (not recommended for production use)
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
        vdoms = item.get("checksum", {}).get("vdoms", {}).keys() if "checksum" in item and "vdoms" in item["checksum"] else []

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


def firmware(api_client: FortigateClient) -> tuple[dict[str, dict[str|int|bool]|list[dict[str, str|int]]], list[Point], list[Point]]:
    """
    Fetches firmware information from the Fortigate API.

    Returns:
        dict[str, dict[str|int|bool]|list[dict[str, str|int]]]: A dictionary containing firmware information.
    """
    res = api_client.get(
        "/api/v2/monitor/system/firmware",
        verify=False # Disable SSL verification for self-signed certificates (not recommended for production use)
    )
    res_json = res.json()

    if not res.ok or "results" not in res_json:
        raise Exception(f"Failed to fetch firmware information: {res.text}")

    # Update available matrics
    firmware_info: dict[str, Any] = res_json.get("results", {})
    current_firmware: dict[str, Any] = firmware_info.get("current", {})
    available_firmware: list[dict[str, Any]] = firmware_info.get("available", [])
    
    last_new_version: dict[str, Any] = available_firmware[0] if available_firmware else {}
    firmware_update_available_points: list[Point] = []
    update_history_firmware_points: list[Point] = []
    
    update_history_firmware_point = (
        Point("firmware_update_history")
        .tag("serial_no", firmware_info.get("serial_no", "unknown"))
        .field("current_version", current_firmware.get("version", "unknown"))
    )
    update_history_firmware_points.append(update_history_firmware_point)
    
    if current_firmware and last_new_version:
        firmware_update_available_point = (
            Point("firmware_update_available")
            .tag("serial_no", firmware_info.get("serial_no", "unknown"))
            .tag("current_version", current_firmware.get("version", "unknown"))
            .tag("available_version", last_new_version.get("version", "unknown"))
            .field("is_update_available", 1 if current_firmware.get("version") != last_new_version.get("version") else 0)
            .field("release_notes", last_new_version.get("release_notes", ""))
        )
        firmware_update_available_points.append(firmware_update_available_point)

    return firmware_info, firmware_update_available_points, update_history_firmware_points


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
    res = api_client.get(
        "/api/v2/monitor/log/device/state",
        verify=False,  # Disable SSL verification for self-signed certificates (not recommended for production use)
    )
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
            point = point.field("overrides_global_faz_cloud", 1 if device_info.get("overrides_global_faz_cloud", False) else 0)

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


def cooperative_security_fabric(
    api_client: FortigateClient,
    vdom: str,
) -> tuple[dict[str, Any], list[Point]]:
    """
    Fetch Cooperative Security Fabric (CSF) information and convert it to
    InfluxDB Points ready for Grafana.

    Parameters
    ----------
    api_client:
        An instance of FortigateClient used to reach the FortiOS REST API.
    vdom:
        The virtual domain context for the API call.

    Returns
    -------
    csf_data:
        Raw parsed results dict (``protocol_enabled`` + ``devices`` list).
    points:
        InfluxDB Points covering device info, state, HA, VDOM counts/features,
        and the global CSF protocol status.

    Raises
    ------
    Exception
        If the HTTP call fails or the response is missing the ``results`` key.
    """
    res = api_client.get(
        "/api/v2/monitor/system/csf",
        params={"vdom": vdom, "scope": "vdom", "decode": "true"},
        verify=False,  # Self-signed cert — replace with a trusted CA in production
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
    fortigate_csf_device_info
    -------------------------
    Static identity + firmware version.  Write on change only (compare
    ``firmware_version_build`` between polls and skip when identical).

    Tags        : serial_no, hostname, model_name, model_number, device_type,
                  ha_mode, ha_group_name
    Fields      : fw_major, fw_minor, fw_patch, fw_build,
                  is_ha_master, ha_group_id, is_vm, limited_ram

    Grafana     : Inventory table, firmware version stat panel,
                  HA role indicator.
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
    """
    """
    serial = device.get("serial", "unknown")

    state: dict[str, Any] = device.get("state", {})

    snapshot_utc = state.get("snapshot_utc_time", 0)
    last_reboot = state.get("utc_last_reboot", 0)
    uptime_ms = max(snapshot_utc - last_reboot, 0)

    point = (
        Point("fortigate_csf_device_uptime")
        .tag("serial_no", serial)
        .field("uptime_ms", int(uptime_ms))
    )
    return [point]
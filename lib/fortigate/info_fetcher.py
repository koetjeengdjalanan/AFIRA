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
        points.extend(_ha_points(device))
        points.append(_vdom_count_point(device))
        points.extend(_vdom_feature_points(device))

    points.append(_protocol_status_point(protocol_enabled, len(fortigate_devices)))

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
    ha_mode = device.get("ha_mode", "unknown")
    ha_group_name = device.get("ha_group_name", "unknown")
    ha_group_id = device.get("ha_group_id", 0)
    is_ha_master = device.get("is_ha_master", 0)

    fw_major = device.get("firmware_version_major", 0)
    fw_minor = device.get("firmware_version_minor", 0)
    fw_patch = device.get("firmware_version_patch", 0)
    fw_build = device.get("firmware_version_build", 0)
    is_vm = device.get("is_vm", False)
    limited_ram = device.get("limited_ram", False)

    point = (
        Point("fortigate_csf_device_info")
        # --- tags (low-cardinality, used for filtering / grouping) ---
        .tag("serial_no", serial)
        .tag("hostname", hostname)
        .tag("model_name", model_name)
        .tag("model_number", model_num)
        .tag("device_type", device_type)
        .tag("ha_mode", str(ha_mode))
        .tag("ha_group_name", ha_group_name)
        # --- fields (numeric/boolean values to plot) ---
        .field("firmware_version_major", int(fw_major))
        .field("firmware_version_minor", int(fw_minor))
        .field("firmware_version_patch", int(fw_patch))
        .field("firmware_version_build", int(fw_build))
        .field("is_ha_master", 1 if is_ha_master else 0)
        .field("ha_group_id", int(ha_group_id))
        .field("is_vm", 1 if is_vm else 0)
        .field("limited_ram", 1 if limited_ram else 0)
    )
    return [point]


def _device_state_point(device: dict[str, Any]) -> list[Point]:
    """
    fortigate_csf_device_state
    --------------------------
    Operational health signals.  Poll every 60 s.

    Tags        : serial_no, hostname, vdom_mode, ngfw_mode, firmware_maturity
    Fields      : conserve_mode, csf_enabled, uptime_ms

    Grafana     : conserve_mode → threshold alert (1 = critical);
                  uptime_ms    → sudden drop detects unexpected reboot;
                  csf_enabled  → security posture stat panel.

    Alert rules (recommended)
    -------------------------
    conserve_mode == 1          → CRITICAL  (device dropping sessions)
    csf_enabled   == 0          → WARNING   (fabric posture gap)
    uptime_ms < previous value  → INFO      (unexpected reboot)
    """
    serial = device.get("serial", "unknown")
    hostname = device.get("host_name", "unknown")

    state: dict[str, Any] = device.get("state", {})
    vdom_mode = state.get("vdom_mode", "unknown")
    ngfw_mode = state.get("ngfw_mode", "unknown")
    conserve_mode = state.get("conserve_mode", False)
    csf_enabled = state.get("csf_enabled", False)
    fw_maturity = device.get("firmware_version_maturity", "unknown")

    snapshot_utc = state.get("snapshot_utc_time", 0)
    last_reboot = state.get("utc_last_reboot", 0)
    uptime_ms = max(snapshot_utc - last_reboot, 0)

    point = (
        Point("fortigate_csf_device_state")
        .tag("serial_no", serial)
        .tag("hostname", hostname)
        .tag("vdom_mode", str(vdom_mode))
        .tag("ngfw_mode", str(ngfw_mode))
        .tag("firmware_maturity", str(fw_maturity))
        .field("conserve_mode", 1 if conserve_mode else 0)
        .field("csf_enabled", 1 if csf_enabled else 0)
        .field("uptime_ms", int(uptime_ms))
    )
    return [point]


def _ha_points(device: dict[str, Any]) -> list[Point]:
    """
    fortigate_csf_ha_cluster  +  fortigate_csf_ha_member
    -----------------------------------------------------
    HA health signals.  Poll every 30 s (HA failover is time-critical).

    Cluster point
    -------------
    Tags    : serial_no, hostname, ha_group_name
    Fields  : ha_member_count, ha_primary_count, ha_secondary_count

    Member point (one per HA member)
    --------------------------------
    Tags    : serial_no, ha_group_name, member_serial, member_hostname, role
    Fields  : member_index, is_primary

    Grafana
    -------
    ha_primary_count alert: should always equal 1.
      == 0  → CRITICAL  (no active primary; traffic may be dropping)
      >= 2  → CRITICAL  (split-brain; dual-active conflict)
    Pair member role tags over time to build a failover event timeline.
    """
    ha_list: list[dict[str, Any]] = device.get("ha_list", [])
    if not ha_list:
        return []

    serial = device.get("serial", "unknown")
    hostname = device.get("host_name", "unknown")
    ha_group_name = device.get("ha_group_name", "unknown")

    ha_member_count = len(ha_list)
    ha_primary_count = sum(
        1 for m in ha_list if m.get("is_ha_primary", False)
    )
    ha_secondary_count = ha_member_count - ha_primary_count

    points: list[Point] = []

    cluster_point = (
        Point("fortigate_csf_ha_cluster")
        .tag("serial_no", serial)
        .tag("hostname", hostname)
        .tag("ha_group_name", ha_group_name)
        .field("ha_member_count", ha_member_count)
        .field("ha_primary_count", ha_primary_count)
        .field("ha_secondary_count", ha_secondary_count)
    )
    points.append(cluster_point)

    for member_idx, member in enumerate(ha_list):
        member_serial = member.get("serial_no", "unknown")
        member_hostname = member.get("hostname",  "unknown")
        is_primary = member.get("is_ha_primary", False)

        member_point = (
            Point("fortigate_csf_ha_member")
            .tag("serial_no", serial)
            .tag("ha_group_name", ha_group_name)
            .tag("member_serial", member_serial)
            .tag("member_hostname", member_hostname)
            .tag("role", "primary" if is_primary else "secondary")
            .field("member_index", member_idx)
            .field("is_primary", 1 if is_primary else 0)
        )
        points.append(member_point)

    return points


def _vdom_count_point(device: dict[str, Any]) -> Point:
    """
    fortigate_csf_vdom_count
    ------------------------
    Number of VDOMs on this device.  Changes rarely; poll every 5 min.

    Tags    : serial_no, hostname
    Fields  : vdom_count

    Grafana : Single-stat panel; alert if count drops unexpectedly.
    """
    serial   = device.get("serial", "unknown")
    hostname = device.get("host_name", "unknown")
    vdoms    = device.get("vdoms", [])

    return (
        Point("fortigate_csf_vdom_count")
        .tag("serial_no", serial)
        .tag("hostname",  hostname)
        .field("vdom_count", len(vdoms))
    )


def _vdom_feature_points(device: dict[str, Any]) -> list[Point]:
    """
    fortigate_csf_vdom_features
    ---------------------------
    Per-VDOM policy mode and administrative role.  Optional — useful for
    config-audit dashboards; low value for real-time monitoring.  Poll
    every 5 min or on change.

    Tags    : serial_no, hostname, vdom_name, ngfw_mode
    Fields  : is_admin_type_vdom, is_management_vdom, transparent_mode,
              central_nat_enabled

    NOTE    : The gui-* feature flags inside vdom_info.features are
              intentionally excluded — they are build-time constants with
              no useful Grafana representation.

    Grafana : Config-audit table; filter by ngfw_mode to spot VDOMs
              running unexpected policy modes.
    """
    serial = device.get("serial", "unknown")
    hostname = device.get("host_name", "unknown")
    vdom_info: dict[str, dict[str, Any]] = device.get("vdom_info", {})

    points: list[Point] = []
    for vdom_name, vdom_details in vdom_info.items():
        if not isinstance(vdom_details, dict):
            continue

        is_admin_type = vdom_details.get("is_admin_type_vdom", False)
        is_management = vdom_details.get("is_management_vdom", False)
        transparent_mode = vdom_details.get("transparent_mode", False)
        ngfw_vdom_mode = vdom_details.get("ngfw_mode", "unknown")
        central_nat = vdom_details.get("central_nat_enabled", False)

        point = (
            Point("fortigate_csf_vdom_features")
            .tag("serial_no", serial)
            .tag("hostname", hostname)
            .tag("vdom_name", vdom_name)
            .tag("ngfw_mode", str(ngfw_vdom_mode))
            .field("is_admin_type_vdom", 1 if is_admin_type else 0)
            .field("is_management_vdom", 1 if is_management else 0)
            .field("transparent_mode", 1 if transparent_mode else 0)
            .field("central_nat_enabled", 1 if central_nat else 0)
        )
        points.append(point)

    return points


def _protocol_status_point(protocol_enabled: bool, device_count: int) -> Point:
    """
    fortigate_csf_protocol_status
    -----------------------------
    Single global row per poll.  Nearly free — always include.  Poll every 60 s.

    Tags    : scope ("global")
    Fields  : protocol_enabled, device_count

    Grafana : Quick global CSF health indicator in the top-level dashboard.
              Alert on protocol_enabled == 0.
    """
    return (
        Point("fortigate_csf_protocol_status")
        .tag("scope", "global")
        .field("protocol_enabled", 1 if protocol_enabled else 0)
        .field("device_count", device_count)
    )

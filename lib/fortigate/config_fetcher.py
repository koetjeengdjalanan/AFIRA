from typing import Any

from influxdb_client.client.write.point import Point

from models import FortigateClient


def system_interface(api_client: FortigateClient) -> tuple[list[dict[str, Any]], list[Point]]:
    """
    Fetches system interface information from the Fortigate API.

    Args:
        api_client (FortigateClient): An instance of the FortigateClient to interact with the API.
        
    Returns:
        tuple[list[dict[str, Any]], list[Point]]: A tuple containing a list of dictionaries with system interface information and a list of InfluxDB points.
    """
    interfaces: list[dict[str, Any]] = []
    points: list[Point] = []
    start = 0

    while True:
        res = api_client.get(
            "/api/v2/cmdb/system/interface",
            params={"start": start, "count": 100},
            verify=False # Disable SSL verification for self-signed certificates (not recommended for production use)
        )
        res_json = res.json()

        if not res.ok or "results" not in res_json:
            raise Exception(f"Failed to fetch system interface information: {res.text}")

        serial_no = res_json.get("serial_no", "unknown")
        size = res_json.get("size", 0)
        matched_count = res_json.get("matched_count", 0)
        next_idx = res_json.get("next_idx", 0)

        for item in res_json.get("results", []):
            interfaces.append({
                "name": item.get("name", ""),
                "vdom": item.get("vdom", "")
            })
            
            point = (
                Point("interface_metrics")
                .tag("interface_name", item.get("name", "unknown"))
                .tag("vdom", item.get("vdom", "unknown"))
                .tag("serial_no", serial_no)
                .field("status", item.get("status", "unknown"))
            )
            points.append(point)

        if len(interfaces) >= size or matched_count == 0:
            break

        start = next_idx

    return interfaces, points


def vdoms(api_client: FortigateClient) -> tuple[list[str], list[Point]]:
    """
    Fetches VDOM information from the Fortigate API.

    Args:
        api_client (FortigateClient): An instance of the FortigateClient to interact with the API.
        
    Returns:
        tuple[list[str], list[Point]]: A tuple containing a list of VDOM names and a list of InfluxDB points.
    """
    vdom_list: list[str] = []
    vdom_history_points: list[Point] = []
    start = 0

    while True:
        res = api_client.get(
            "/api/v2/cmdb/system/vdom",
            params={"start": start, "count": 100},
            verify=False # Disable SSL verification for self-signed certificates (not recommended for production use)
        )
        res_json = res.json()

        if not res.ok or "results" not in res_json:
            raise Exception(f"Failed to fetch VDOM information: {res.text}")

        size = res_json.get("size", 0)
        matched_count = res_json.get("matched_count", 0)
        next_idx = res_json.get("next_idx", 0)
        
        point = (
            Point("vdom_count")
            .tag("serial_no", res_json.get("serial_no", "unknown"))
            .field("count", len(res_json.get("results", [])))
        )
        vdom_history_points.append(point)

        for item in res_json.get("results", []):
            vdom_list.append(item.get("name", ""))

        if len(vdom_list) >= size or matched_count == 0:
            break

        start = next_idx

    return vdom_list, vdom_history_points


def firewall_traffic_shapper(api_client: FortigateClient, vdom: str) -> list[dict[str, str]]:
    """
    Fetches firewall traffic shaper information from the Fortigate API.

    Args:
        api_client (FortigateClient): An instance of the FortigateClient to interact with the API.
        vdom (str): The VDOM for which to fetch the traffic shaper information.
        
    Returns:
        list[dict[str, str]]: A list of dictionaries containing firewall traffic shaper information.
    """
    pass


def sdwan_health_check(api_client: FortigateClient, vdom: str) -> list[dict[str, str]]:
    """
    Fetches SD-WAN health check information from the Fortigate API.

    Args:
        api_client (FortigateClient): An instance of the FortigateClient to interact with the API.
        vdom (str): The VDOM for which to fetch the SD-WAN health check information.
        
    Returns:
        list[dict[str, str]]: A list of dictionaries containing SD-WAN health check information.
    """
    pass


def firmware(api_client: FortigateClient) -> tuple[dict[str, dict[str|int|bool]|list[dict[str, str|int]]], list[Point], list[Point]]:
    """
    Fetches firmware information from the Fortigate API.

    Args:
        api_client (FortigateClient): An instance of the FortigateClient to interact with the API.

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


def log_device_state(api_client: FortigateClient) -> dict[str, str|bool|dict[str, int|bool]]:
    """
    Fetches log device state information from the Fortigate API.

    Args:
        api_client (FortigateClient): An instance of the FortigateClient to interact with the API.
    
    Returns:
        dict[str, str|bool|dict[str, int|bool]]: A dictionary containing log device state information.
    """
    pass


def cooperative_security_fabric(api_client: FortigateClient):
    """
    Fetches cooperative security fabric information from the Fortigate API.

    Args:
        api_client (FortigateClient): An instance of the FortigateClient to interact with the API.
    """ 
    pass

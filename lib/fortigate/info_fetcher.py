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


def firmware(api_client: FortigateClient) -> dict[str, dict[str|int|bool]|list[dict[str, str|int]]]:
    """
    Fetches firmware information from the Fortigate API.

    Args:
        api_client (FortigateClient): An instance of the FortigateClient to interact with the API.

    Returns:
        dict[str, dict[str|int|bool]|list[dict[str, str|int]]]: A dictionary containing firmware information.
    """
    pass


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

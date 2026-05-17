"""Collection of functions for fetching and processing device details from the HPE API."""

from collections.abc import Mapping
from typing import cast

from influxdb_client.client.write.point import Point

from models import HPEOAuth2Client


def _mapping_or_empty(value: object) -> Mapping[str, object]:
    """Return mapping values unchanged and normalize malformed nested sections to an empty mapping."""
    if isinstance(value, Mapping):
        return cast("Mapping[str, object]", value)
    return {}


def _int_or_default(value: object, default: int = 0) -> int:
    """Convert API numeric values to int, falling back when they are missing or malformed."""
    if not isinstance(value, int | float | str | bytes | bytearray):
        return default

    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _first_number_or_default(value: object, default: float = 0.0) -> float:
    """Read the first value from an API sample list and convert it to float."""
    if not isinstance(value, list) or len(value) == 0:
        return default

    first_value: object = value[0]
    if not isinstance(first_value, int | float | str | bytes | bytearray):
        return default

    try:
        return float(first_value)
    except (TypeError, ValueError):
        return default


def ap_data(api_client: HPEOAuth2Client, serial_number: str) -> list[Point]:
    """Fetch access point details and convert them into an InfluxDB point.

    Args:
        api_client: Authenticated HPE OAuth2 API client used to perform the GET request. The client must expose a
            `get` method compatible with `requests.Response`, including the `ok`, `text`, `url`, and `json()` members
            used by this function.
        serial_number: Access point serial number used to address the HPE Aruba Networking Central AP details endpoint.

    Returns:
        A single-item list containing an InfluxDB `Point` for the `access_point_metrics` measurement.

        The generated point contains these tags:
            - `serial_number`: AP serial number from `serialNumber`, or `"unknown"` when absent.
            - `site_id`: Site identifier from `siteId`, or `"unknown"` when absent.
            - `mac_address`: AP MAC address from `macAddress`, or `"unknown"` when absent.
            - `device_name`: AP display name from `deviceName`, or `"unknown"` when absent.
            - `model`: AP model from `model`, or `"unknown"` when absent.
            - `role`: AP role from `role`, or `"unknown"` when absent.
            - `deployment`: Deployment mode/state from `deployment`, or `"unknown"` when absent.

        The generated point contains these fields:
            - `status`: AP status from `status`, or `"unknown"` when absent.
            - `ipv4`: IPv4 address from `ipv4`, or `"unknown"` when absent.
            - `ipv6`: IPv6 address from `ipv6`, or `"unknown"` when absent.
            - `public_ipv4`: Public IPv4 address from `publicIpv4`, or `"unknown"` when absent.
            - `uptime_in_millis`: Uptime value from `uptimeInMillis`, or `0` when absent.
            - `last_reboot_reason`: Last reboot reason from `lastRebootReason`, or `"unknown"` when absent.
            - `last_seen_at`: Last-seen timestamp from `lastSeenAt`, or `"unknown"` when absent.
            - `software_version`: Installed software version from `softwareVersion`, or `"unknown"` when absent.
            - `manufacturer`: Manufacturer name from `manufacturer`, or `"unknown"` when absent.
            - `negotiated_power`: Negotiated power value from `negotiatedPower`, or `"unknown"` when absent.
            - `band_selection`: Band selection value from `bandSelection`, or `"unknown"` when absent.
            - `country_code`: Country code from `countryCode`, or `"unknown"` when absent.
            - `mode`: AP mode from `mode`, or `"unknown"` when absent.
            - `default_gateway`: Default gateway from `defaultGateway`, or `"unknown"` when absent.

    Raises:
        ConnectionError: Raised when the API response is unsuccessful.

    Notes:
        Missing AP attributes are tolerated and represented with default values so the point can still be written to
        InfluxDB when optional API fields are absent.
    """
    res = api_client.get(
        endpoint=f"/network-monitoring/v1/aps/{serial_number}",
        headers={"Accept": "application/json"},
    )

    if not res.ok:
        raise ConnectionError(
            f"[ap_data] - unexpected response for ap serial number {serial_number}. Response: {res.text}", res.url
        )

    data: Mapping[str, object] = _mapping_or_empty(res.json())
    point: Point = (
        Point("access_point_metrics")
        .tag("serial_number", serial_number)
        .tag("site_id", data.get("siteId", "unknown"))
        .tag("mac_address", data.get("macAddress", "unknown"))
        .tag("device_name", data.get("deviceName", "unknown"))
        .tag("model", data.get("model", "unknown"))
        .tag("role", data.get("role", "unknown"))
        .tag("deployment", data.get("deployment", "unknown"))
        .field("status", data.get("status", "unknown"))
        .field("ipv4", data.get("ipv4", "unknown"))
        .field("ipv6", data.get("ipv6", "unknown"))
        .field("public_ipv4", data.get("publicIpv4", "unknown"))
        .field("uptime_in_millis", data.get("uptimeInMillis", 0))
        .field("last_reboot_reason", data.get("lastRebootReason", "unknown"))
        .field("last_seen_at", data.get("lastSeenAt", "unknown"))
        .field("software_version", data.get("softwareVersion", "unknown"))
        .field("manufacturer", data.get("manufacturer", "unknown"))
        .field("negotiated_power", data.get("negotiatedPower", "unknown"))
        .field("band_selection", data.get("bandSelection", "unknown"))
        .field("country_code", data.get("countryCode", "unknown"))
        .field("mode", data.get("mode", "unknown"))
        .field("default_gateway", data.get("defaultGateway", "unknown"))
    )

    return [point]


def ap_cpu_util(api_client: HPEOAuth2Client, serial_number: str) -> list[Point]:
    """Fetch access point CPU utilization trend samples and convert them into InfluxDB points.

    Args:
        api_client: Authenticated HPE OAuth2 API client used to perform the GET request. The client must expose a
            `get` method compatible with `requests.Response`, including the `ok`, `text`, `url`, and `json()` members
            used by this function.
        serial_number: Access point serial number used to address the CPU utilization trends endpoint.

    Returns:
        A list of InfluxDB `Point` objects using the `access_point_metrics` measurement. Each point is tagged with
        `serial_number`, contains the `cpu_utilization` field converted to `float`, and uses the sample `timestamp` as
        the point time.

    Raises:
        ConnectionError: Raised when the API response is unsuccessful or the response body does not contain the
            top-level `graph` key expected from the CPU utilization trends endpoint.
        TypeError: Raised implicitly when a sample `data` value cannot be indexed as expected.
        ValueError: Raised implicitly when the selected sample data value cannot be converted to `float`.

    Notes:
        The function reads `graph.samples` and uses the first value in each sample's `data` list. If a sample omits
        `data`, the CPU utilization defaults to `0.0`.
    """
    points: list[Point] = []
    res = api_client.get(
        endpoint=f"/network-monitoring/v1/aps/{serial_number}/cpu-utilization-trends",
        headers={"Accept": "application/json"},
    )

    data: Mapping[str, object] = _mapping_or_empty(res.json())

    if not res.ok or "graph" not in data:
        raise ConnectionError(
            f"[ap_cpu_util] - unexpected response for ap serial number {serial_number}. Response: {res.text}", res.url
        )

    graph: Mapping[str, object] = _mapping_or_empty(data.get("graph"))
    samples: object = graph.get("samples", [])

    if not isinstance(samples, list):
        samples = []

    for sample_value in samples:
        sample: Mapping[str, object] = _mapping_or_empty(sample_value)
        point = (
            Point("access_point_metrics")
            .tag("serial_number", serial_number)
            .field("cpu_utilization", _first_number_or_default(sample.get("data")))
            .time(sample.get("timestamp", ""))
        )
        points.append(point)

    return points


def ap_mem_util(api_client: HPEOAuth2Client, serial_number: str) -> list[Point]:
    """Fetch access point memory utilization trend samples and convert them into InfluxDB points.

    Args:
        api_client: Authenticated HPE OAuth2 API client used to perform the GET request. The client must expose a
            `get` method compatible with `requests.Response`, including the `ok`, `text`, `url`, and `json()` members
            used by this function.
        serial_number: Access point serial number used to address the memory utilization trends endpoint.

    Returns:
        A list of InfluxDB `Point` objects using the `access_point_metrics` measurement. Each point is tagged with
        `serial_number`, contains the `memory_utilization` field converted to `float`, and uses the sample `timestamp`
        as the point time.

    Raises:
        ConnectionError: Raised when the API response is unsuccessful or the response body does not contain the
            top-level `graph` key expected from the memory utilization trends endpoint.
        TypeError: Raised implicitly when a sample `data` value cannot be indexed as expected.
        ValueError: Raised implicitly when the selected sample data value cannot be converted to `float`.

    Notes:
        The function reads `graph.samples` and uses the first value in each sample's `data` list. If a sample omits
        `data`, the memory utilization defaults to `0.0`.
    """
    points: list[Point] = []
    res = api_client.get(
        endpoint=f"/network-monitoring/v1/aps/{serial_number}/memory-utilization-trends",
        headers={"Accept": "application/json"},
    )

    data: Mapping[str, object] = _mapping_or_empty(res.json())

    if not res.ok or "graph" not in data:
        raise ConnectionError(
            f"[ap_mem_util] - unexpected response for ap serial number {serial_number}. Response: {res.text}", res.url
        )

    graph: Mapping[str, object] = _mapping_or_empty(data.get("graph"))
    samples: object = graph.get("samples", [])

    if not isinstance(samples, list):
        samples = []

    for sample_value in samples:
        sample: Mapping[str, object] = _mapping_or_empty(sample_value)
        point = (
            Point("access_point_metrics")
            .tag("serial_number", serial_number)
            .field("memory_utilization", _first_number_or_default(sample.get("data")))
            .time(sample.get("timestamp", ""))
        )
        points.append(point)

    return points


def ap_power_util(api_client: HPEOAuth2Client, serial_number: str) -> list[Point]:
    """Fetch access point power consumption trend samples and convert them into InfluxDB points.

    Args:
        api_client: Authenticated HPE OAuth2 API client used to perform the GET request. The client must expose a
            `get` method compatible with `requests.Response`, including the `ok`, `text`, `url`, and `json()` members
            used by this function.
        serial_number: Access point serial number used to address the power consumption trends endpoint.

    Returns:
        A list of InfluxDB `Point` objects using the `access_point_metrics` measurement. Each point is tagged with
        `serial_number`, contains the `power_utilization` field converted to `float`, and uses the sample `timestamp`
        as the point time.

    Raises:
        ConnectionError: Raised when the API response is unsuccessful or the response body does not contain the
            top-level `graph` key expected from the power consumption trends endpoint.
        TypeError: Raised implicitly when a sample `data` value cannot be indexed as expected.
        ValueError: Raised implicitly when the selected sample data value cannot be converted to `float`.

    Notes:
        The function reads `graph.samples` and uses the first value in each sample's `data` list. If a sample omits
        `data`, the power utilization defaults to `0.0`.
    """
    points: list[Point] = []
    res = api_client.get(
        endpoint=f"/network-monitoring/v1/aps/{serial_number}/power-consumption-trends",
        headers={"Accept": "application/json"},
    )

    data: Mapping[str, object] = _mapping_or_empty(res.json())

    if not res.ok or "graph" not in data:
        raise ConnectionError(
            f"[ap_power_util] - unexpected response for ap serial number {serial_number}. Response: {res.text}", res.url
        )

    graph: Mapping[str, object] = _mapping_or_empty(data.get("graph"))
    samples: object = graph.get("samples", [])

    if not isinstance(samples, list):
        samples = []

    for sample_value in samples:
        sample: Mapping[str, object] = _mapping_or_empty(sample_value)
        point = (
            Point("access_point_metrics")
            .tag("serial_number", serial_number)
            .field("power_utilization", _first_number_or_default(sample.get("data")))
            .time(sample.get("timestamp", ""))
        )
        points.append(point)

    return points


# TODO: There still tunnel data that needs to be gathered if needed!


def switch_data(api_client: HPEOAuth2Client, serial_number: str) -> list[Point]:
    """Fetch switch details and convert them into an InfluxDB point.

    Args:
        api_client: Authenticated HPE OAuth2 API client used to perform the GET request. The client must expose a
            `get` method compatible with `requests.Response`, including the `ok`, `text`, `url`, and `json()` members
            used by this function.
        serial_number: Switch serial number used to address the HPE Aruba Networking Central switch details endpoint.

    Returns:
        A single-item list containing an InfluxDB `Point` for the `switch_metrics` measurement.

        The generated point contains these tags:
            - `serial_number`: Switch serial number from `serial`, or `"unknown"` when absent.
            - `site_id`: Site identifier from `siteId`, or `"unknown"` when absent.
            - `device_name`: Switch display name from `deviceName`, or `"unknown"` when absent.
            - `model`: Switch model from `model`, or `"unknown"` when absent.
            - `mac_address`: Switch MAC address from `macAddress`, or `"unknown"` when absent.
            - `deployment`: Deployment mode/state from `deployment`, or `"unknown"` when absent.
            - `switch_role`: Switch role from `switchRole`, or `"unknown"` when absent.
            - `manufacturer`: Manufacturer name from `manufacturer`, or `"unknown"` when absent.

        The generated point contains these fields:
            - `status`: Switch status from `status`, or `"unknown"` when absent.
            - `health`: Switch health from `health`, or `"unknown"` when absent.
            - `health_reasons`: Comma-separated poor health reasons from `healthReasons.poorReasons`, or an empty
                string when absent.
            - `ipv4`: IPv4 address from `ipv4`, or `"unknown"` when absent.
            - `ipv6`: IPv6 address from `ipv6`, or `"unknown"` when absent.
            - `public_ip`: Public IP address from `publicIp`, or `"unknown"` when absent.
            - `uptime`: Uptime value from `upTime`, or `0` when absent.
            - `firmware_version`: Installed firmware version from `firmwareVersion`, or `"unknown"` when absent.
            - `last_seen`: Last-seen timestamp from `lastSeen`, or `0` when absent.
            - `config_status`: Configuration status from `configStatus`, or `"unknown"` when absent.
            - `last_config_change`: Last configuration change timestamp from `lastConfigChange`, or `0` when absent.
            - `last_restart_reason`: Last restart reason from `lastRestartReason`, or `"unknown"` when absent.
            - `stack_member_id`: Stack member identifier from `stackMemberId`, or `0` when absent.
            - `stack_member_status`: Stack member status from `stackMemberStatus`, or `"unknown"` when absent.
            - `stack_member_priority`: Stack member priority from `stackMemberPriority`, or `0` when absent.

    Raises:
        ConnectionError: Raised when the API response is unsuccessful.

    Notes:
        Missing switch attributes are tolerated and represented with default values so the point can still be written
        to InfluxDB when optional API fields are absent.
    """
    points: list[Point] = []
    res = api_client.get(
        endpoint=f"/network-monitoring/v1/switches/{serial_number}",
        headers={"Accept": "application/json"},
    )

    if not res.ok:
        raise ConnectionError(
            f"[switch_data] - unexpected response for switch serial number {serial_number}. Response: {res.text}",
            res.url,
        )

    data: Mapping[str, object] = _mapping_or_empty(res.json())
    health_reasons: Mapping[str, object] = _mapping_or_empty(data.get("healthReasons"))
    poor_reasons: object = health_reasons.get("poorReasons", [])

    if not isinstance(poor_reasons, list):
        poor_reasons = []

    point: Point = (
        Point("switch_metrics")
        .tag("serial_number", serial_number)
        .tag("site_id", data.get("siteId", "unknown"))
        .tag("device_name", data.get("deviceName", "unknown"))
        .tag("model", data.get("model", "unknown"))
        .tag("mac_address", data.get("macAddress", "unknown"))
        .tag("deployment", data.get("deployment", "unknown"))
        .tag("switch_role", data.get("switchRole", "unknown"))
        .tag("manufacturer", data.get("manufacturer", "unknown"))
        .field("status", data.get("status", "unknown"))
        .field("health", data.get("health", "unknown"))
        .field("health_reasons", ", ".join(str(reason) for reason in poor_reasons))
        .field("ipv4", data.get("ipv4", "unknown"))
        .field("ipv6", data.get("ipv6", "unknown"))
        .field("public_ip", data.get("publicIp", "unknown"))
        .field("uptime", data.get("upTime", 0))
        .field("firmware_version", data.get("firmwareVersion", "unknown"))
        .field("last_seen", data.get("lastSeen", 0))
        .field("config_status", data.get("configStatus", "unknown"))
        .field("last_config_change", data.get("lastConfigChange", 0))
        .field("last_restart_reason", data.get("lastRestartReason", "unknown"))
        .field("stack_member_id", data.get("stackMemberId", 0))
        .field("stack_member_status", data.get("stackMemberStatus", "unknown"))
        .field("stack_member_priority", data.get("stackMemberPriority", 0))
    )
    points.append(point)

    for trend in cast(list[dict], data.get("switchTrends", [])):
        trend_data: Mapping[str, object] = _mapping_or_empty(trend)
        point = (
            Point("switch_metrics")
            .tag("serial_number", serial_number)
            .tag("device_name", data.get("deviceName", "unknown"))
            .field("usage", trend_data.get("usage", 0))
            .field("system_temperature", trend_data.get("systemTemperature", 0))
            .field("memory_utilization", trend_data.get("memoryUtilization", 0))
            .field("poe_consumption", trend_data.get("poeConsumption", 0))
            .field("total_power_consumption", trend_data.get("totalPowerConsumption", 0))
            .field("up_link_ports", trend_data.get("upLinkPorts", "[]"))
            .field("cpu_utilization", trend_data.get("cpuUtilization", 0))
            .field("power_consumption", trend_data.get("powerConsumption", 0))
            .field("switch_role", trend_data.get("switchRole", "unknown"))
            .field("poe_available", trend_data.get("poeAvailable", 0))
        )
        points.append(point)

    return points


def switch_hw_data(api_client: HPEOAuth2Client, serial_number: str) -> list[Point]:
    """Fetch switch hardware category details and convert them into InfluxDB points.

    Args:
        api_client: Authenticated HPE OAuth2 API client used to perform the GET request. The client must expose a
            `get` method compatible with `requests.Response`, including the `ok`, `text`, `url`, and `json()` members
            used by this function.
        serial_number: Switch serial number used to address the hardware categories endpoint.

    Returns:
        A list of InfluxDB `Point` objects using the `switch_metrics` measurement. One point is created for each item
        in the API response.

        Each generated point contains these tags:
            - `serial_number`: Hardware item serial number from `serial`, or `"unknown"` when absent.
            - `model`: Hardware item model from `model`, or `"unknown"` when absent.
            - `role`: Hardware item role from `role`, or `"unknown"` when absent.

        Each generated point contains these fields:
            - `cpu_health`: CPU health from `cpu.health`, or `"unknown"` when absent.
            - `memory_health`: Memory health from `memory.health`, or `"unknown"` when absent.
            - `temperature_health`: Temperature health from `temperature.health`, or `"unknown"` when absent.
            - `fans_health`: Fan health from `fans.health`, or `"unknown"` when absent.
            - `fans_total_count`: Total fan count from `fans.totalCount`, or `0` when absent.
            - `fans_up_count`: Up fan count from `fans.upCount`, or `0` when absent.
            - `power_supplies_health`: Power supply health from `powerSupplies.health`, or `"unknown"` when absent.
            - `power_supplies_total_count`: Total power supply count from `powerSupplies.totalCount`, or `0` when
                absent.
            - `power_supplies_up_count`: Up power supply count from `powerSupplies.upCount`, or `0` when absent.
            - `poe_slots_health`: PoE slot health from `poeSlots.health`, or `"unknown"` when absent.
            - `poe_slots_total_count`: Total PoE slot count from `poeSlots.totalCount`, or `0` when absent.
            - `poe_slots_up_count`: Up PoE slot count from `poeSlots.upCount`, or `0` when absent.
            - `stack_member_id`: Stack member identifier from `stackMemberId`, or `"unknown"` when absent.
            - `status`: Hardware item status from `status`, or `"unknown"` when absent.

    Raises:
        ConnectionError: Raised when the API response is unsuccessful or the response body does not contain the
            top-level `items` key expected from the hardware categories endpoint.

    Notes:
        The function emits one point per returned hardware category item. Nested health sections default to
        `"unknown"` and nested count values default to `0` when their parent section is absent.
    """
    points: list[Point] = []
    res = api_client.get(
        endpoint=f"/network-monitoring/v1/switches/{serial_number}/hardware-categories",
        headers={"Accept": "application/json"},
    )

    data: Mapping[str, object] = _mapping_or_empty(res.json())

    if not res.ok or "items" not in data:
        raise ConnectionError(
            f"[switch_hw_data] - unexpected response for switch serial number {serial_number}. Response: {res.text}",
            res.url,
        )

    items: object = data.get("items", [])

    if not isinstance(items, list):
        items = []

    for item_value in items:
        item: Mapping[str, object] = _mapping_or_empty(item_value)
        cpu: Mapping[str, object] = _mapping_or_empty(item.get("cpu"))
        memory: Mapping[str, object] = _mapping_or_empty(item.get("memory"))
        temperature: Mapping[str, object] = _mapping_or_empty(item.get("temperature"))
        fans: Mapping[str, object] = _mapping_or_empty(item.get("fans"))
        power_supplies: Mapping[str, object] = _mapping_or_empty(item.get("powerSupplies"))
        poe_slots: Mapping[str, object] = _mapping_or_empty(item.get("poeSlots"))

        point = (
            Point("switch_metrics")
            .tag("serial_number", serial_number)
            .tag("model", item.get("model", "unknown"))
            .tag("role", item.get("role", "unknown"))
            .field("cpu_health", cpu.get("health", "unknown"))
            .field("memory_health", memory.get("health", "unknown"))
            .field("temperature_health", temperature.get("health", "unknown"))
            .field("fans_health", fans.get("health", "unknown"))
            .field("fans_total_count", _int_or_default(fans.get("totalCount", 0)))
            .field("fans_up_count", _int_or_default(fans.get("upCount", 0)))
            .field("power_supplies_health", power_supplies.get("health", "unknown"))
            .field("power_supplies_total_count", _int_or_default(power_supplies.get("totalCount", 0)))
            .field("power_supplies_up_count", _int_or_default(power_supplies.get("upCount", 0)))
            .field("poe_slots_health", poe_slots.get("health", "unknown"))
            .field("poe_slots_total_count", _int_or_default(poe_slots.get("totalCount", 0)))
            .field("poe_slots_up_count", _int_or_default(poe_slots.get("upCount", 0)))
            .field("stack_member_id", item.get("stackMemberId", "unknown"))
            .field("status", item.get("status", "unknown"))
        )
        points.append(point)
    return points


def gateways_hw_data(api_client: HPEOAuth2Client, serial_number: str) -> list[Point]:
    """
    Gateways hardware details.

    Fetch gateway hardware data from the HPE Network Monitoring API for a given serial number
    and return it as a list containing a single InfluxDB point.

    Parameters:
        api_client (HPEOAuth2Client): Authenticated API client for HPE OAuth2 requests.
        serial_number (str): Serial number of the gateway to retrieve.

    Returns:
        list[Point]: A list with one Point object populated with gateway metadata and status fields.

    Raises:
        ConnectionError: If the API response is not successful or contains no JSON payload.
    """
    res = api_client.get(
        endpoint=f"/network-monitoring/v1/gateways/{serial_number}", headers={"Accept": "application/json"}
    )

    if not res.ok or not res.json():
        raise ConnectionError(
            f"[gateways_hw_data] - unexpected response for gateway serial number {serial_number}. Response: {res.text}",
            res.url,
        )

    data = res.json()

    point = (
        Point("gateway_metrics")
        .tag("serial_number", serial_number)
        .tag("site_id", data.get("siteId", "unknown"))
        .tag("site_name", data.get("siteName", "unknown"))
        .tag("device_name", data.get("deviceName", "unknown"))
        .tag("role", data.get("role", "unknown"))
        .tag("deployment", data.get("deployment", "unknown"))
        .tag("cluster_name", data.get("clusterName", "unknown"))
        .tag("model", data.get("model", "unknown"))
        .tag("mac_address", data.get("macAddress", "unknown"))
        .tag("persona", data.get("persona", "unknown"))
        .field("status", data.get("status", "unknown"))
        .field("public_ipv4", data.get("publicIpv4", "unknown"))
        .field("ipv4", data.get("ipv4", "unknown"))
        .field("ipv6", data.get("ipv6", "unknown"))
        .field("uptime_in_millis", data.get("uptimeInMillis", 0))
        .field("software_version", data.get("softwareVersion", "unknown"))
        .field("last_restart_reason", data.get("lastRestartReason", "unknown"))
        .field("manufacturer", data.get("manufacturer", "unknown"))
        .field("part_number", data.get("partNumber", "unknown"))
        .field("failure_reason", data.get("failureReason", "unknown"))
    )

    return [point]


def ap_radio(api_client: HPEOAuth2Client, serial_number: str) -> list[Point]:
    """
    Retrieve radio metrics for a given access point and convert each radio entry into an InfluxDB Point.

    Parameters:
        api_client : HPEOAuth2Client
            Authenticated client used to call the HPE network-monitoring API.
        serial_number : str
            Serial number of the access point whose radio data will be fetched.

    Returns:
        list[Point]
            A list of Point objects with measurement name "radio_metrics". Each Point contains tags
            (e.g. serial_number, device_name, mac_address, site_id, band, mode, radio_number, radio_id)
            and fields for radio metrics (e.g. health, status, band_range, channel, bandwidth, power,
            channel_utilization, non_wifi_interference, tx_utilization, rx_utilization, noise_floor,
            errors, drops, retries, channel_quality, channel_change_count, power_change_count).

    Raises:
        ConnectionError
            If the API response is not successful, contains no JSON body, or returns an empty list.

    Notes:
        - The function expects the endpoint "/network-monitoring/v1/aps/{serial_number}/radios" to
        return a JSON array of radio objects. Missing or absent attributes are defaulted as in the
        implementation (using dict.get).
    """
    points: list[Point] = []
    res = api_client.get(
        endpoint=f"/network-monitoring/v1/aps/{serial_number}/radios",
        headers={"Accept": "application/json"},
    )

    if not res.ok or not res.json() or len(res.json().get("items", [])) < 1:
        raise ConnectionError(
            f"[radio_data] - unexpected response for ap serial number {serial_number}. Response: {res.text}", res.url
        )

    for radio in res.json().get("items", []):
        point = (
            Point("radio_metrics")
            .tag("serial_number", serial_number)
            .tag("mac_address", radio.get("macAddress", "unknown"))
            .tag("site_id", radio.get("siteId", "unknown"))
            .tag("band", radio.get("band", "unknown"))
            .tag("mode", radio.get("mode", "unknown"))
            .tag("radio_number", radio.get("radioNumber", -1))
            .tag("radio_id", radio.get("id", "unknown"))
            .field("health", radio.get("health", "unknown"))
            .field("status", radio.get("status", "unknown"))
            .field("band_range", radio.get("bandRange", "unknown"))
            .field("channel", radio.get("channel", "unknown"))
            .field("bandwidth", radio.get("bandwidth", "unknown"))
            .field("power", radio.get("power", 0))
            .field("channel_utilization", radio.get("channelUtilization", 0))
            .field("non_wifi_interference", radio.get("nonWifiInterference", 0))
            .field("tx_utilization", radio.get("txUtilization", 0))
            .field("rx_utilization", radio.get("rxUtilization", 0))
            .field("noise_floor", radio.get("noiseFloor", 0))
            .field("errors", radio.get("errors", 0))
            .field("drops", float(radio.get("drops", 0)))
            .field("retries", radio.get("retries", 0))
            .field("channel_quality", radio.get("channelQuality", 0))
            .field("channel_change_count", radio.get("channelChangeCount", 0))
            .field("power_change_count", radio.get("powerChangeCount", 0))
        )
        points.append(point)

    return points

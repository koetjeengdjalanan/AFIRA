"""Collection of functions to fetch data from the API."""

from typing import Any

from influxdb_client.client.write.point import Point
from requests.models import Response

from models import HPEOAuth2Client


def site_health(api_client: HPEOAuth2Client) -> tuple[list[str], list[Point]]:
    """Fetch site health summaries and convert them into InfluxDB points.

    This function reads the HPE Aruba Networking Central site-health endpoint and transforms each returned site into a
    `Point` suitable for writing to InfluxDB. It also returns the site identifiers separately so callers can use the
    same API response to drive follow-up requests or cross-reference other datasets.

    Args:
        api_client: Authenticated HPE OAuth2 API client used to perform the GET request. The client is expected to
            expose a `get` method compatible with `requests.Response`, including the `ok`, `status_code`, `text`, and
            `json()` members used by this function.

    Returns:
        A tuple containing:
            - A list of site IDs in the same order as the processed API response.
            - A list of InfluxDB `Point` objects using the `site_health_metrics` measurement.

        Each generated point contains these tags:
            - `site_id`: Unique site identifier from the API.
            - `site_name`: Human-readable site name.
            - `address`: Street address, or `"unknown"` when absent.
            - `country`: Country name/code, or `"unknown"` when absent.
            - `city`: City name, or `"unknown"` when absent.
            - `state`: State or region, or `"unknown"` when absent.
            - `zip_code`: Postal code, or `"unknown"` when absent.
            - `full_address`: Concatenated address in `address, city, state, zip_code, country` order.

        Each generated point contains these fields:
            - `total_alerts`: Total alert count reported for the site.
            - `critical_alerts`: Alert count for the `Critical` alert group, or `0` when that group is missing.
            - `health_poor`: Site health value for the `Poor` group, or `0` when missing.
            - `health_fair`: Site health value for the `Fair` group, or `0` when missing.
            - `health_good`: Site health value for the `Good` group, or `0` when missing.
            - `device_count`: Total device count reported for the site.
            - `device_health_poor`: Device health value for the `Poor` group, or `0` when missing.
            - `device_health_fair`: Device health value for the `Fair` group, or `0` when missing.
            - `device_health_good`: Device health value for the `Good` group, or `0` when missing.
            - `client_count`: Total client count reported for the site.
            - `client_health_poor`: Client health value for the `Poor` group, or `0` when missing.
            - `client_health_fair`: Client health value for the `Fair` group, or `0` when missing.
            - `client_health_good`: Client health value for the `Good` group, or `0` when missing.
            - `longitude`: Site longitude converted to `float`, or `0.0` when absent or empty.
            - `latitude`: Site latitude converted to `float`, or `0.0` when absent or empty.

    Raises:
        ValueError: Raised when the API response is unsuccessful or when the response body does not contain the
            top-level `sites` key expected from `network-monitoring/v1/sites-health`.
        KeyError: Raised implicitly when a required nested site field is missing from an otherwise accepted response,
            such as `id`, `name`, `alerts`, `health`, `devices`, or `clients`.
        TypeError: Raised implicitly when nested values have an unexpected type, such as `address` not being a mapping.

    Notes:
        The endpoint is requested once and is not paginated here. Optional groups use a default value of `0`, while
        several core fields are treated as required because they are necessary to produce meaningful monitoring points.
    """
    res = api_client.get("network-monitoring/v1/sites-health", headers={"Accept": "application/json"})

    if not res.ok or "items" not in res.json():
        raise ValueError(f"[site_health] - Unexpected API response: {res.status_code} - {res.text}", res.url)

    points: list[Point] = []
    site_ids: list[str] = []
    for site in res.json().get("items", []):
        site_id = site["id"]
        site_ids.append(site_id)
        site_name = site["siteName"]
        address = site["address"].get("address", "unknown")
        country = site["address"].get("country", "unknown")
        city = site["address"].get("city", "unknown")
        state = site["address"].get("state", "unknown")
        zip_code = site["address"].get("zipCode", "unknown")
        total_alerts = site["alerts"]["totalCount"]
        critical_alerts = next((group["count"] for group in site["alerts"]["groups"] if group["name"] == "Critical"), 0)

        # Extract health values for Poor, Fair, and Good categories
        health_poor = next((group["value"] for group in site["health"]["groups"] if group["name"] == "Poor"), 0)
        health_fair = next((group["value"] for group in site["health"]["groups"] if group["name"] == "Fair"), 0)
        health_good = next((group["value"] for group in site["health"]["groups"] if group["name"] == "Good"), 0)

        device_count = site["devices"]["count"]
        device_health_poor = next(
            (group["value"] for group in site["devices"]["health"]["groups"] if group["name"] == "Poor"), 0
        )
        device_health_fair = next(
            (group["value"] for group in site["devices"]["health"]["groups"] if group["name"] == "Fair"), 0
        )
        device_health_good = next(
            (group["value"] for group in site["devices"]["health"]["groups"] if group["name"] == "Good"), 0
        )

        client_count = site["clients"]["count"]
        client_health_poor = next(
            (group["value"] for group in site["clients"]["health"]["groups"] if group["name"] == "Poor"), 0
        )
        client_health_fair = next(
            (group["value"] for group in site["clients"]["health"]["groups"] if group["name"] == "Fair"), 0
        )
        client_health_good = next(
            (group["value"] for group in site["clients"]["health"]["groups"] if group["name"] == "Good"), 0
        )

        # Extract location data
        location = site.get("location", {})
        longitude = float(location.get("longitude", 0.0)) if location.get("longitude") else 0.0
        latitude = float(location.get("latitude", 0.0)) if location.get("latitude") else 0.0

        # Concatenate the full address
        full_address = f"{address}, {city}, {state}, {zip_code}, {country}"

        # Create a point for InfluxDB
        point = (
            Point("site_health_metrics")
            .tag("site_id", site_id)
            .tag("site_name", site_name)
            .tag("address", address)
            .tag("country", country)
            .tag("city", city)
            .tag("state", state)
            .tag("zip_code", zip_code)
            .tag("full_address", full_address)
            .field("total_alerts", total_alerts)
            .field("critical_alerts", critical_alerts)
            .field("health_poor", health_poor)
            .field("health_fair", health_fair)
            .field("health_good", health_good)
            .field("device_count", device_count)
            .field("device_health_poor", device_health_poor)
            .field("device_health_fair", device_health_fair)
            .field("device_health_good", device_health_good)
            .field("client_count", client_count)
            .field("client_health_poor", client_health_poor)
            .field("client_health_fair", client_health_fair)
            .field("client_health_good", client_health_good)
            .field("longitude", longitude)
            .field("latitude", latitude)
        )
        points.append(point)

    return site_ids, points


def device_data(api_client: HPEOAuth2Client) -> tuple[list[dict[str, Any]], list[Point]]:
    """Fetch network devices and convert them into inventory records plus InfluxDB points.

    This function reads the paginated HPE Aruba Networking Central devices endpoint. It returns a compact device list
    for application-level use and a richer set of `Point` objects for time-series storage in InfluxDB.

    Pagination is handled by reading the `next` value from each API response. When `next` is a string containing an
    integer greater than `1`, the function requests another page using the same `limit` and the parsed `next` cursor.
    Pagination stops when `next` is missing, is not a string, or resolves to a value that is not greater than `1`.

    Args:
        api_client: Authenticated HPE OAuth2 API client used to perform all GET requests. The client is expected to
            expose a `get` method compatible with `requests.Response`, including the `ok`, `status_code`, `text`, and
            `json()` members used by this function.

    Returns:
        A tuple containing:
            - A list of compact device dictionaries. Each dictionary contains `serial_number`, `device_type`, and
                `site_id`, with `"unknown"` used for any missing value.
            - A list of InfluxDB `Point` objects using the `network_metrics` measurement.

        Each compact device dictionary contains:
            - `serial_number`: Device serial number from `serialNumber`, or `"unknown"` when absent.
            - `device_type`: Device category/type from `deviceType`, or `"unknown"` when absent.
            - `site_id`: Site identifier from `siteId`, or `"unknown"` when absent.

        Each generated point contains these tags:
            - `device_id`: Device ID from `id`, or `"unknown"` when absent.
            - `mac_address`: Device MAC address from `macAddress`, or `"unknown"` when absent.
            - `site_id`: Site identifier from `siteId`, or `"unknown"` when absent.
            - `device_type`: Device category/type from `deviceType`, or `"unknown"` when absent.
            - `device_name`: Device display name from `deviceName`, or `"unknown"` when absent.
            - `model`: Device model from `model`, or `"unknown"` when absent.
            - `serial_number`: Device serial number from `serialNumber`, or `"unknown"` when absent.
            - `deployment`: Deployment mode/state from `deployment`, or `"unknown"` when absent.

        Each generated point contains these fields:
            - `status`: Device status from `status`, or `"unknown"` when absent.
            - `ipv4`: IPv4 address from `ipv4`, or `"unknown"` when absent.
            - `ipv6`: IPv6 address from `ipv6`, or `"unknown"` when absent.
            - `software_version`: Installed software version from `softwareVersion`, or `"unknown"` when absent.
            - `uptime_in_millis`: Uptime value from `uptimeInMillis`, or `"unknown"` when absent.
            - `last_seen_at`: Last-seen timestamp from `lastSeenAt`, or `"unknown"` when absent.
            - `role`: Device role from `role`, or `"unknown"` when absent.

    Raises:
        ValueError: Raised when the first API response is unsuccessful, does not contain the top-level `items` key, or
            reports a `count` of `0`. Also raised when any paginated response is unsuccessful or lacks `items`.
        ValueError: Raised implicitly when the API returns a string `next` value that cannot be parsed as an integer.

    Notes:
        The first page is requested with `limit=100`. Missing per-device attributes are tolerated and represented as
        `"unknown"` in both the compact dictionary output and the InfluxDB point. An empty first page is treated as an
        invalid response because the function is intended to collect current device inventory data.
    """
    devices: list[dict[str, Any]] = []
    points: list[Point] = []

    res_collection: list[Response] = [
        api_client.get(
            endpoint="/network-monitoring/v1/devices", headers={"Accept": "application/json"}, params={"limit": 100}
        )
    ]

    if any(
        [
            not res_collection[0].ok,
            "items" not in res_collection[0].json(),
            res_collection[0].json().get("count", 0) == 0,
        ]
    ):
        raise ValueError(
            f"[device_data] - Unexpected API response: {res_collection[0].status_code} - {res_collection[0].text}",
            res_collection[0].url,
        )
    _: str | None = res_collection[0].json().get("next", None)
    next_page = int(_) if isinstance(_, str) else None

    while isinstance(next_page, int) and next_page > 1:
        res = api_client.get(
            endpoint="/network-monitoring/v1/devices",
            headers={"Accept": "application/json"},
            params={"limit": 100, "next": next_page},
        )
        if not res.ok or "items" not in res.json():
            raise ValueError(
                f"[device_data] - Unexpected API response on page {next_page}: {res.status_code} - {res.text}", res.url
            )
        res_collection.append(res)
        next_page = int(res.json().get("next", 0)) if isinstance(res.json().get("next", None), str) else None

    for res in res_collection:
        for item in res.json().get("items", []):
            devices.append(
                {
                    "serial_number": item.get("serialNumber", "unknown"),
                    "device_type": item.get("deviceType", "unknown"),
                    "site_id": item.get("siteId", "unknown"),
                }
            )
            point = (
                Point("network_metrics")
                .tag(key="device_id", value=item.get("id", "unknown"))
                .tag(key="mac_address", value=item.get("macAddress", "unknown"))
                .tag(key="site_id", value=item.get("siteId", "unknown"))
                .tag(key="device_type", value=item.get("deviceType", "unknown"))
                .tag(key="device_name", value=item.get("deviceName", "unknown"))
                .tag(key="model", value=item.get("model", "unknown"))
                .tag(key="serial_number", value=item.get("serialNumber", "unknown"))
                .tag(key="deployment", value=item.get("deployment", "unknown"))
                .field(field="status", value=item.get("status", "unknown"))
                .field(field="ipv4", value=item.get("ipv4", "unknown"))
                .field(field="ipv6", value=item.get("ipv6", "unknown"))
                .field(field="software_version", value=item.get("softwareVersion", "unknown"))
                .field(field="uptime_in_millis", value=item.get("uptimeInMillis", "unknown"))
                .field(field="last_seen_at", value=item.get("lastSeenAt", "unknown"))
                .field(field="role", value=item.get("role", "unknown"))
            )
            points.append(point)
    return devices, points


def wlan_data(api_client: HPEOAuth2Client) -> tuple[list[str], list[Point]]:
    """Fetch WLAN definitions and convert them into WLAN names plus InfluxDB points.

    This function reads the paginated HPE Aruba Networking Central WLAN endpoint. It returns the WLAN names separately
    for simple caller-side matching/reporting and returns detailed `Point` objects for writing WLAN configuration
    metadata to InfluxDB.

    Pagination is handled by reading the `next` value from each API response. When `next` is a string containing an
    integer greater than `1`, the function requests another page using the same `limit` and the parsed `next` cursor.
    Pagination stops when `next` is missing, is not a string, or resolves to a value that is not greater than `1`.

    Args:
        api_client: Authenticated HPE OAuth2 API client used to perform all GET requests. The client is expected to
            expose a `get` method compatible with `requests.Response`, including the `ok`, `status_code`, `text`, and
            `json()` members used by this function.

    Returns:
        A tuple containing:
            - A list of WLAN names from `wlanName`, with `"unknown"` used for missing names.
            - A list of InfluxDB `Point` objects using the `wlan_metrics` measurement.

        Each generated point contains these tags:
            - `wlan_name`: WLAN display name from `wlanName`, or `"unknown"` when absent.
            - `primary_usage`: Primary WLAN usage classification from `primaryUsage`, or `"unknown"` when absent.
            - `security_level`: Security level from `securityLevel`, or `"unknown"` when absent.
            - `security`: Security mode from `security`, or `"unknown"` when absent.
            - `band`: Radio band from `band`, or `"unknown"` when absent.
            - `status`: WLAN status from `status`, or `"unknown"` when absent.
            - `type`: WLAN type from `type`, or `"unknown"` when absent.

        Each generated point contains these fields:
            - `vlan`: VLAN value from `vlan`, or `"unknown"` when absent.
            - `id`: WLAN identifier from `id`, or `"unknown"` when absent.

    Raises:
        ValueError: Raised when the first API response is unsuccessful, does not contain the top-level `items` key, or
            reports a `count` of `0`. Also raised when any paginated response is unsuccessful or lacks `items`.
        ValueError: Raised implicitly when the API returns a string `next` value that cannot be parsed as an integer.

    Notes:
        The first page is requested with `limit=100`. Missing per-WLAN attributes are tolerated and represented as
        `"unknown"` in the WLAN-name list and in the InfluxDB point. An empty first page is treated as an invalid
        response because the function is intended to collect current WLAN configuration data.
    """
    wlan_names: list[str] = []
    points: list[Point] = []

    res_collection: list[Response] = [
        api_client.get(
            endpoint="/network-monitoring/v1/wlans", headers={"Accept": "application/json"}, params={"limit": 100}
        )
    ]

    if any(
        [
            not res_collection[0].ok,
            "items" not in res_collection[0].json(),
            res_collection[0].json().get("count", 0) == 0,
        ]
    ):
        raise ValueError(
            f"[wlan_data] - Unexpected API response: {res_collection[0].status_code} - {res_collection[0].text}",
            res_collection[0].url,
        )
    _: str | None = res_collection[0].json().get("next", None)
    next_page = int(_) if isinstance(_, str) else None

    while isinstance(next_page, int) and next_page > 1:
        res = api_client.get(
            endpoint="/network-monitoring/v1/wlans",
            headers={"Accept": "application/json"},
            params={"limit": 100, "next": next_page},
        )
        if not res.ok or "items" not in res.json():
            raise ValueError(
                f"[wlan_data] - Unexpected API response on page {next_page}: {res.status_code} - {res.text}", res.url
            )
        res_collection.append(res)
        next_page = int(res.json().get("next", 0)) if isinstance(res.json().get("next", None), str) else None

    for res in res_collection:
        for item in res.json().get("items", []):
            wlan_names.append(item.get("wlanName", "unknown"))

            point = (
                Point(measurement_name="wlan_metrics")
                .tag(key="wlan_name", value=item.get("wlanName", "unknown"))
                .tag(key="primary_usage", value=item.get("primaryUsage", "unknown"))
                .tag(key="security_level", value=item.get("securityLevel", "unknown"))
                .tag(key="security", value=item.get("security", "unknown"))
                .tag(key="band", value=item.get("band", "unknown"))
                .tag(key="status", value=item.get("status", "unknown"))
                .tag(key="type", value=item.get("type", "unknown"))
                .field(field="vlan", value=item.get("vlan", "unknown"))
                .field(field="id", value=item.get("id", "unknown"))
            )
            points.append(point)

    return wlan_names, points

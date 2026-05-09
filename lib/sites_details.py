"""Functions to fetch site details from HPE Aruba Central API."""

import datetime
from zoneinfo import ZoneInfo

import requests
from influxdb_client.client.write.point import Point
from requests.models import Response

from models import HPEOAuth2Client


def device_locations(api_client: HPEOAuth2Client, site_id: str) -> list[Point]:
    """Fetch device location records for a site and convert them into InfluxDB points.

    Args:
        api_client: Authenticated HPE OAuth2 API client used to request the
            site device-location endpoint.
        site_id: Site identifier used in the endpoint path.

    Returns:
        A list of InfluxDB `Point` objects using the
        `device_location_metrics` measurement. Each point contains device,
        site, floor, identity, deployment, status, and location-source tags,
        plus floor, IP address, creation timestamp, location timestamp,
        longitude, and latitude fields.

    Raises:
        ValueError: Raised when the API response is unsuccessful or does not
            contain an `items` collection.

    Notes:
        Missing string attributes are recorded as `"unknown"`. Invalid numeric
        floor and coordinate values are coerced to `0` or `0.0` so malformed
        location payloads do not prevent the remaining devices from being
        exported.
    """
    points: list[Point] = []

    res = api_client.get(
        endpoint=f"/network-services/v1/sites/{site_id}/device-locations",
        headers={"Accept": "application/json"},
    )

    if not res.ok or "items" not in res.json():
        raise ValueError(
            "[device_locations] - Failed to fetch device locations for site "
            f"{site_id}: {res.status_code} - {res.text}",
            res.url,
        )

    for device in res.json().get("items", []):
        device_id: str = device.get("id", "unknown")
        serial_number: str = device.get("serialNumber", "unknown")
        device_type: str = device.get("type", "unknown")
        site_id_tag: str = device.get("siteId", "unknown")
        building_id: str = device.get("buildingId", "unknown")
        floor_id: str = device.get("floorId", "unknown")

        try:
            floor_level = int(device.get("floorLevel", 0))
        except (ValueError, TypeError):
            floor_level = 0

        tenant_id: str = device.get("tenantId", "unknown")
        ipv4: str = device.get("ipv4", "unknown")
        ipv6: str = device.get("ipv6", "unknown")
        mac_address: str = device.get("macAddress", "unknown")
        model: str = device.get("model", "unknown")
        deployment: str = device.get("deployment", "unknown")
        status: str = device.get("status", "unknown")
        created_at: str = device.get("createdAt", "unknown")

        # Extract consolidated location information with error handling
        consolidated_location: dict = device.get("consolidatedLocation", {})
        source: str = consolidated_location.get("source", "unknown")
        timestamp: str = consolidated_location.get("timestamp", "unknown")

        # Geographic center (latitude/longitude) with error handling
        center: dict = consolidated_location.get("center", {})
        if center is None:
            center = {}

        # Ensure coordinates are always floats
        try:
            longitude = (
                float(center.get("longitude", 0.0))
                if isinstance(center, dict) and center.get("longitude") is not None
                else 0.0
            )
        except (ValueError, TypeError):
            longitude = 0.0

        try:
            latitude = (
                float(center.get("latitude", 0.0))
                if isinstance(center, dict) and center.get("latitude") is not None
                else 0.0
            )
        except (ValueError, TypeError):
            latitude = 0.0

        point: Point = (
            Point("device_location_metrics")
            .tag("device_id", device_id)
            .tag("serial_number", serial_number)
            .tag("device_type", device_type)
            .tag("site_id", site_id_tag)
            .tag("building_id", building_id)
            .tag("floor_id", floor_id)
            .tag("tenant_id", tenant_id)
            .tag("mac_address", mac_address)
            .tag("model", model)
            .tag("deployment", deployment)
            .tag("status", status)
            .tag("location_source", source)
            .field("floor_level", floor_level)
            .field("ipv4", ipv4)
            .field("ipv6", ipv6)
            .field("created_at", created_at)
            .field("location_timestamp", timestamp)
            .field("longitude", longitude)
            .field("latitude", latitude)
        )

        points.append(point)
    return points


def wifi_clients_loc(api_client: HPEOAuth2Client, site_id: str) -> list[Point]:
    """Fetch latest Wi-Fi client locations and convert them into points.

    Args:
        api_client: Authenticated HPE OAuth2 API client used to request Wi-Fi
            client location data.
        site_id: Site identifier used in the API filter expression.

    Returns:
        A list of InfluxDB `Point` objects using the
        `wifi_client_location_metrics` measurement. Each point contains client
        identity, site, building, floor, MAC, association, and classification
        tags, plus association state, accuracy, reporting AP count, connection
        state, creation timestamp, latitude, and longitude fields.

    Raises:
        ValueError: Raised inside the request loop when the API response is
            unsuccessful, lacks `items`, or returns an empty `items`
            collection. The exception is caught by the surrounding request
            handler, logged, and ends the fetch loop.

    Notes:
        The request asks for the latest location per client with `limit=1000`.
        The current implementation requests offset `0` repeatedly and relies
        on the returned `total` and processed count to decide when to stop.
        Per-client processing failures are printed and skipped so one malformed
        client does not block the rest.
    """
    points: list[Point] = []

    total_clients = None
    clients_fetched = 0

    while True:
        try:
            res = api_client.get(
                endpoint="/network-services/v1/wifi-clients-locations",
                headers={"Accept": "application/json"},
                params={
                    "filter": f"siteId eq '{site_id}'",
                    "latest-location-per-client": 1,
                    "limit": 1000,
                    "offset": 0,
                },
            )

            if not res.ok or "items" not in res.json() or len(res.json().get("items", [])) < 1:
                raise ValueError(
                    "[wifi_clients_loc] - Failed to fetch Wi-Fi client "
                    f"locations for site {site_id}: {res.status_code} - "
                    f"{res.text}",
                    res.url,
                )
            res.raise_for_status()
            data = res.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching WiFi clients location data for site {site_id}: {e}")
            break

        if not data or "items" not in data or data["count"] == 0:
            print(f"No more WiFi clients location data to fetch for site {site_id}.")
            break

        # Set total_clients from the first response
        if total_clients is None:
            total_clients = data.get("total", 0)
            print(f"Total WiFi clients for site {site_id}: {total_clients}")

        # Check if we've already fetched all clients
        if clients_fetched >= total_clients:
            print(f"All {total_clients} WiFi clients have been fetched for site " f"{site_id}")
            break

        points = []
        for client in data["items"]:
            try:
                # Extract basic client information
                client_id = client.get("id", "unknown")
                site_id_tag = client.get("siteId", "unknown")
                building_id = client.get("buildingId", "unknown")
                floor_id = client.get("floorId", "unknown")
                mac_address = client.get("macAddress", "unknown")
                hashed_mac_address = client.get("hashedMacAddress", "unknown")
                associated = client.get("associated", False)
                associated_bssid = client.get("associatedBssid", "unknown")
                client_classification = client.get("clientClassification", "unknown")
                accuracy = client.get("accuracy", 0.0)
                num_reporting_aps = client.get("numOfReportingAps", 0)
                connected = client.get("connected", False)
                created_at = client.get("createdAt", "unknown")

                # Extract geographic coordinates with error handling
                geo_coordinates = client.get("geoCoordinates")
                if geo_coordinates is None:
                    geo_coordinates = {}

                latitude = geo_coordinates.get("latitude", 0.0) if isinstance(geo_coordinates, dict) else 0.0
                longitude = geo_coordinates.get("longitude", 0.0) if isinstance(geo_coordinates, dict) else 0.0

                # Create InfluxDB point for WiFi client location
                point = (
                    Point("wifi_client_location_metrics")
                    .tag("client_id", client_id)
                    .tag("site_id", site_id_tag)
                    .tag("building_id", building_id)
                    .tag("floor_id", floor_id)
                    .tag("mac_address", mac_address)
                    .tag("hashed_mac_address", hashed_mac_address)
                    .tag("associated_bssid", associated_bssid)
                    .tag("client_classification", client_classification)
                    .field("associated", associated)
                    .field("accuracy", accuracy)
                    .field("num_reporting_aps", num_reporting_aps)
                    .field("connected", connected)
                    .field("created_at", created_at)
                    .field("latitude", latitude)
                    .field("longitude", longitude)
                )

                points.append(point)
            except Exception as e:
                print(
                    "Error processing WiFi client location data for client "
                    f"{client.get('id', 'unknown')} in site {site_id}: {e}"
                )
                print("Skipping this client and continuing with the next one.")
                continue
    return points


def clients_data(api_client: HPEOAuth2Client, site_id: str) -> list[Point]:
    """Fetch network clients for a site and convert them into InfluxDB points.

    Args:
        api_client: Authenticated HPE OAuth2 API client used to request network
            client data.
        site_id: Site identifier passed as the `site-id` query parameter.

    Returns:
        A list of InfluxDB `Point` objects using the `client_metrics`
        measurement. Each point contains client identity, connection, role,
        experience, status, network, tunnel, security, authentication, and
        capability tags, plus IP, port, VLAN, tunnel ID, last-seen, and
        connected-since fields.

    Raises:
        ValueError: Raised when the first API response is unsuccessful, does
            not contain `items`, or returns an empty `items` collection.
        ValueError: Raised implicitly when a string `next` value cannot be
            parsed as an integer.

    Notes:
        Pagination follows the response `next` value as an offset. Later request
        failures or empty pages are printed and stop pagination, while records
        collected before that point are still converted and returned. Missing
        client attributes are recorded as `"unknown"` or `0` depending on the
        target field.
    """
    points: list[Point] = []

    res_collection: list[Response] = [
        api_client.get(
            endpoint="/network-monitoring/v1/clients",
            headers={"Accept": "application/json"},
            params={"site-id": site_id, "limit": 1000, "offset": 0},
        )
    ]

    if any(
        [
            not res_collection[0].ok,
            "items" not in res_collection[0].json(),
            len(res_collection[0].json().get("items", [])) < 1,
        ]
    ):
        raise ValueError(
            "[clients_data] - Failed to fetch clients data for site "
            f"{site_id}: {res_collection[0].status_code} - "
            f"{res_collection[0].text}",
            res_collection[0].url,
        )

    _: str | None = res_collection[0].json().get("next", None)
    next_page = int(_) if isinstance(_, str) else None

    while isinstance(next_page, int):
        try:
            res_collection.append(
                api_client.get(
                    endpoint="/network-monitoring/v1/clients",
                    headers={"Accept": "application/json"},
                    params={"site-id": site_id, "limit": 1000, "offset": next_page},
                )
            )
        except requests.exceptions.RequestException as e:
            print(f"Error fetching clients data for site {site_id} at offset " f"{next_page}: {e}")
            break

        if (
            not res_collection[-1].ok
            or "items" not in res_collection[-1].json()
            or len(res_collection[-1].json().get("items", [])) < 1
        ):
            print(f"No more clients data to fetch for site {site_id} at offset " f"{next_page}.")
            break

        _: str | None = res_collection[-1].json().get("next", None)
        next_page = int(_) if isinstance(_, str) else None

    for res in res_collection:
        for client in res.json().get("items", []):
            point = (
                Point(measurement_name="client_metrics")
                .tag(key="mac_address", value=client.get("mac", "unknown"))
                .tag(key="name", value=client.get("name", "unknown"))
                .tag(key="type", value=client.get("type", "unknown"))
                .tag(key="role", value=client.get("role", "unknown"))
                .tag(key="experience", value=client.get("experience", "unknown"))
                .tag(key="status", value=client.get("status", "unknown"))
                .tag(key="status_reason", value=client.get("statusReason", "unknown"))
                .tag(
                    key="connected_device_serial",
                    value=client.get("connectedDeviceSerial", "unknown"),
                )
                .tag(key="connected_to", value=client.get("connectedTo", "unknown"))
                .tag(key="network", value=client.get("network", "unknown"))
                .tag(key="tunnel", value=client.get("tunnel", "unknown"))
                .tag(key="key_management", value=client.get("keyManagement", "unknown"))
                .tag(
                    key="authentication",
                    value=client.get("authentication", "unknown"),
                )
                .tag(key="capabilities", value=client.get("capabilities", "unknown"))
                .field(field="ipv4", value=client.get("ipv4", "unknown"))
                .field(field="ipv6", value=client.get("ipv6", "unknown"))
                .field(field="port", value=client.get("port", "unknown"))
                .field(field="vlan_id", value=client.get("vlanId", "unknown"))
                .field(field="tunnel_id", value=client.get("tunnelId", 0))
                .field(field="last_seen_at", value=client.get("lastSeenAt", "unknown"))
                .field(
                    field="connected_since",
                    value=client.get("connectedSince", "unknown"),
                )
            )
            points.append(point)
    return points


def web_app_data(api_client: HPEOAuth2Client, site_id: str) -> list[Point]:
    """Fetch web application usage for a site and convert it into InfluxDB points.

    The query window covers the seven days ending at the current time in the
    `Asia/Jakarta` timezone, converted to UTC ISO-8601 timestamps before being
    sent to the API.

    Args:
        api_client: Authenticated HPE OAuth2 API client used to request
            application monitoring data.
        site_id: Site identifier passed as the `site-id` query parameter and
            stored as a point tag.

    Returns:
        A list of InfluxDB `Point` objects using the `web_app_metrics`
        measurement. Each point contains application ID, name, categories, risk,
        state, host type, destination country, and site tags, plus transmitted
        bytes, received bytes, Poor/Fair/Good experience counts, and last-used
        timestamp fields.

    Raises:
        ValueError: Raised when the first API response is unsuccessful, lacks
            `applicationsV1.items`, or reports an application count of `0`.
        ValueError: Raised implicitly when numeric response values such as
            `total`, `count`, or `lastUsedTime` cannot be converted to integers.

    Notes:
        Pagination continues until the gathered item count reaches the reported
        total or a later page is invalid. Missing optional application
        attributes are recorded with conservative defaults such as `"unknown"`,
        `"UNKNOWN"`, `0`, or an empty category string.
    """
    points: list[Point] = []
    end_time = datetime.datetime.now(ZoneInfo("Asia/Jakarta")).astimezone(datetime.UTC)
    start_time = end_time - datetime.timedelta(days=7)

    res_collection: list[Response] = [
        api_client.get(
            endpoint="/network-monitoring/v1/applications",
            headers={"Accept": "application/json"},
            params={
                "site-id": site_id,
                "start-at": start_time.isoformat(timespec="seconds").replace("+00:00", "Z"),
                "end-at": end_time.isoformat(timespec="seconds").replace("+00:00", "Z"),
                "limit": 1000,
                "offset": 0,
            },
        )
    ]

    if any(
        [
            not res_collection[0].ok,
            "items" not in res_collection[0].json().get("applicationsV1", {}),
            res_collection[0].json().get("applicationsV1", {}).get("count", 0) == 0,
        ]
    ):
        raise ValueError(
            "[web_app_data] - Failed to fetch web application data for site "
            f"{site_id}: {res_collection[0].status_code} - "
            f"{res_collection[0].text}",
            res_collection[0].url,
        )

    total = int(res_collection[0].json().get("applicationsV1", {}).get("total", 0))
    gathered = int(res_collection[0].json().get("applicationsV1", {}).get("count", 0))

    while all([isinstance(total, int), isinstance(gathered, int), total > gathered]):
        res_collection.append(
            api_client.get(
                endpoint="/network-monitoring/v1/applications",
                headers={"Accept": "application/json"},
                params={
                    "site-id": site_id,
                    "start-at": start_time.isoformat(timespec="seconds").replace("+00:00", "Z"),
                    "end-at": end_time.isoformat(timespec="seconds").replace("+00:00", "Z"),
                    "limit": 1000,
                    "offset": gathered,
                },
            )
        )

        if (
            not res_collection[-1].ok
            or "items" not in res_collection[-1].json().get("applicationsV1", {})
            or res_collection[-1].json().get("applicationsV1", {}).get("count", 0) == 0
        ):
            break

        gathered += int(res_collection[-1].json().get("applicationsV1", {}).get("count", 0))

    for res in res_collection:
        for app in res.json().get("applicationsV1", {}).get("items", []):
            app_id = app.get("id", "unknown")
            app_name = app.get("name", "unknown")
            categories = ", ".join(app.get("categories", []))
            risk = app.get("risk", "UNKNOWN")
            tx_bytes = app.get("txBytes", 0)
            rx_bytes = app.get("rxBytes", 0)
            state = app.get("state", "UNKNOWN")
            last_used_time = app.get("lastUsedTime", "0")
            application_host_type = app.get("applicationHostType", "UNKNOWN")
            dest_country = (
                app.get("destLocation", [{}])[0].get("countryName", "UNKNOWN") if app.get("destLocation") else "UNKNOWN"
            )

            # Extract experience values for Poor, Fair, and Good categories
            experience_poor = next(
                (group["count"] for group in app.get("experience", {}).get("groups", []) if group["name"] == "Poor"),
                0,
            )
            experience_fair = next(
                (group["count"] for group in app.get("experience", {}).get("groups", []) if group["name"] == "Fair"),
                0,
            )
            experience_good = next(
                (group["count"] for group in app.get("experience", {}).get("groups", []) if group["name"] == "Good"),
                0,
            )

            # Create a point for InfluxDB
            point = (
                Point("web_app_metrics")
                .tag("app_id", app_id)
                .tag("app_name", app_name)
                .tag("categories", categories)
                .tag("risk", risk)
                .tag("state", state)
                .tag("application_host_type", application_host_type)
                .tag("dest_country", dest_country)
                .tag("site_id", site_id)
                .field("tx_bytes", tx_bytes)
                .field("rx_bytes", rx_bytes)
                .field("experience_poor", experience_poor)
                .field("experience_fair", experience_fair)
                .field("experience_good", experience_good)
                .field("last_used_time", int(last_used_time))
            )
        points.append(point)
    return points


def sitemap(api_client: HPEOAuth2Client, site_id: str) -> list[Point]:
    """Placeholder for fetching a site map and converting it into InfluxDB points.

    Args:
        api_client: Authenticated HPE OAuth2 API client reserved for the future
            sitemap request.
        site_id: Site identifier reserved for the future sitemap request.

    Returns:
        A list of InfluxDB `Point` objects when implemented.

    Raises:
        NotImplementedError: Always raised because sitemap export has not been
            implemented yet.
    """
    raise NotImplementedError("The sitemap function is not implemented yet.")
    # points: list[Point] = []
    # return points


def wlan_trhougput_trends(api_client: HPEOAuth2Client, wlan_name: str) -> list[Point]:
    """Fetch WLAN throughput trend samples and convert them into InfluxDB points.

    Args:
        api_client: Authenticated HPE OAuth2 API client used to request WLAN
            throughput trend data.
        wlan_name: WLAN name used in the endpoint path and stored as a point tag.

    Returns:
        A list of InfluxDB `Point` objects using the `wlan_throughput_metrics`
        measurement. Each sample is expanded into one point per graph key,
        tagged with the WLAN name, fielded with the corresponding key/value
        pair, and timestamped with the sample timestamp.

    Raises:
        ValueError: Raised when the API response is unsuccessful, lacks a
            `graph` object, or has no usable sample data.

    Notes:
        The function name preserves the current project spelling,
        `wlan_trhougput_trends`, for compatibility with existing imports and
        callers.
    """
    points: list[Point] = []
    res = api_client.get(
        f"/network-monitoring/v1/wlans/{wlan_name}/throughput-trends",
        headers={"Accept": "application/json"},
    )

    if any(
        [
            not res.ok,
            "graph" not in res.json(),
            res.json().get("graph", {}).get("samples", []) == 0,
        ]
    ):
        raise ValueError(
            "[wlan_trhougput_trends] - Failed to fetch throughput trends for "
            f"WLAN {wlan_name}: {res.status_code} - {res.text}",
            res.url,
        )

    for sample in res.json().get("graph", {}).get("samples", []):
        for key, value in zip(res.json()["graph"]["keys"], sample["data"]):
            point = (
                Point(measurement_name="wlan_throughput_metrics")
                .tag(key="wlan_name", value=wlan_name)
                .field(field=key, value=value)
                .time(sample.get("timestamp", ""))
            )
            points.append(point)

    return points

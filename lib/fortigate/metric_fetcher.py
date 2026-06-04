import json
from typing import Any

from influxdb_client.client.write.point import Point
from influxdb_client.domain.write_precision import WritePrecision

from models import FortigateClient


def system_resource_usage(
    api_client: FortigateClient,
    vdom: str | None = None,
    scope: str = "global",
) -> tuple[list[dict[str, Any]], list[Point]]:
    """
    Fetches system resource usage information from the Fortigate API.

    Args:
        api_client (FortigateClient): An instance of the FortigateClient to interact with the API.
        vdom (str | None): The VDOM for which to fetch the system resource usage information.
        scope (str): The resource usage scope.

    Returns:
        tuple[list[dict[str, Any]], list[Point]]: A tuple containing system resource usage dictionaries and InfluxDB points.
    """
    params: dict[str, str] = {"scope": scope}
    if vdom:
        params["vdom"] = vdom

    res = api_client.get(
        "/api/v2/monitor/system/resource/usage",
        params=params,
        verify=False # Disable SSL verification for self-signed certificates (not recommended for production use)
    )
    res_json = res.json()

    if not res.ok or "results" not in res_json:
        raise Exception(f"Failed to fetch system resource usage information: {res.text}")

    results = res_json.get("results", {})
    response_vdom = res_json.get("vdom", vdom or scope)
    serial_no = res_json.get("serial", "unknown")
    resource_items: list[dict[str, Any]] = []
    points: list[Point] = []

    for resource_name, resource_entries in results.items():
        if not isinstance(resource_entries, list):
            continue

        for resource_index, resource_entry in enumerate(resource_entries):
            if not isinstance(resource_entry, dict):
                continue

            current = resource_entry.get("current", 0)

            resource_items.append({
                "resource": resource_name,
                "resource_index": resource_index,
                "vdom": response_vdom,
                "serial": serial_no,
                "current": current
            })

            point = (
                Point("fortigate_system_resource_usage")
                .tag("serial_no", serial_no)
                .tag("vdom", response_vdom)
                .tag("resource", resource_name)
                .tag("resource_index", str(resource_index))
                .field("current", current)
            )
            points.append(point)

    return resource_items, points


def license_status(api_client: FortigateClient) -> tuple[list[dict[str, Any]], list[Point]]:
    """
    Fetches currently licensed FortiGuard services with version and expiration data.

    Args:
        api_client (FortigateClient): An instance of the FortigateClient to interact with the API.
        vdom (str): The VDOM for which to fetch the license status information.

    Returns:
        tuple[list[dict[str, Any]], list[Point]]: A tuple containing current license dictionaries and a list of InfluxDB points.
    """
    def normalize_key(key: str) -> str:
        return key.replace("-", "_").replace(" ", "_")

    def scalar_value(value: Any) -> str | int | float | None:
        if isinstance(value, bool):
            return 1 if value else 0
        if isinstance(value, (str, int, float)):
            return value
        return None

    def expiration_fields(fields: dict[str, str | int | float]) -> dict[str, str | int | float]:
        return {
            key: value
            for key, value in fields.items()
            if ("expires" == key)
        }

    def is_currently_used_license(fields: dict[str, str | int | float]) -> bool:
        status = str(fields.get("status", "")).strip().lower()
        return status == "licensed" and bool(expiration_fields(fields))

    def flatten_license_service(
        service: str,
        value: dict[str, Any],
        *,
        parent_path: str = "",
    ) -> None:
        component = f"{parent_path}.{service}" if parent_path else service
        fields: dict[str, str | int | float] = {}
        nested_items: list[tuple[str, dict[str, Any]]] = []

        for key, item_value in value.items():
            if isinstance(item_value, dict):
                nested_items.append((key, item_value))
                continue

            field_value = scalar_value(item_value)
            if field_value is not None:
                fields[normalize_key(key)] = field_value

        current_expiration_fields: dict[str, int] = expiration_fields(fields)

        if fields:
            record = {
                "serial": serial_no,
                "service": service if not parent_path else parent_path.split(".")[0],
                "component": component,
                "expires": current_expiration_fields.get("expires", 0),
            }
            if "version" in fields:
                record["version"] = fields["version"]
            license_items.append(record)

            point = (
                Point("fortigate_license_status")
                .tag("serial_no", serial_no)
                .tag("service", record["service"])
                .tag("component", component)
                .tag("type", str(fields.get("type", "unknown")))
                .tag("status", str(fields.get("status", "unknown")))
                .tag("entitlement", str(fields.get("entitlement", "unknown")))
            )

            if "version" in fields:
                point = point.field("version", fields["version"])

            for field_key, field_value in current_expiration_fields.items():
                point = point.field(field_key, field_value)

            points.append(point)

        for nested_key, nested_value in nested_items:
            flatten_license_service(nested_key, nested_value, parent_path=component)

    res = api_client.get(
        "/api/v2/monitor/license/status",
        verify=False # Disable SSL verification for self-signed certificates (not recommended for production use)
    )
    res_json = res.json()

    if not res.ok or "results" not in res_json:
        raise Exception(f"Failed to fetch license status information: {res.text}")

    results = res_json.get("results", {})
    serial_no = res_json.get("serial", "unknown")
    license_items: list[dict[str, Any]] = []
    points: list[Point] = []

    for service, value in results.items():
        if isinstance(value, dict):
            flatten_license_service(service, value)

    return license_items, points


def vwan_health_check(api_client: FortigateClient, vdom: str, sla_configuration: dict[str, Any]) -> tuple[list[dict[str, Any]], list[Point]]:
    """
    Fetches virtual WAN health check information from the Fortigate API.

    Args:
        api_client (FortigateClient): An instance of the FortigateClient to interact with the API.
        vdom (str): The VDOM for which to fetch the virtual WAN health check information.
        sla_configuration (dict[str, Any]): The SLA configuration dictionary.

    Returns:
        tuple[list[dict[str, Any]], list[Point]]: A tuple containing a flattened list of virtual WAN health check dictionaries and a list of InfluxDB points.
    """
    res = api_client.get(
        "/api/v2/monitor/virtual-wan/health-check",
        params={"vdom": vdom},
        verify=False # Disable SSL verification for self-signed certificates (not recommended for production use)
    )
    res_json = res.json()

    if not res.ok or "results" not in res_json:
        raise Exception(f"Failed to fetch virtual WAN health check information: {res.text}")

    results = res_json.get("results", {})
    response_vdom = res_json.get("vdom", vdom)
    serial_no = res_json.get("serial", "unknown")
    health_checks: list[dict[str, Any]] = []
    points: list[Point] = []

    for health_check_name, members in results.items():
        if not isinstance(members, dict):
            continue

        sla_configuration_entry = sla_configuration.get(health_check_name, {})
        sla_thresholds = sla_configuration_entry.get("sla_thresholds", {}) if isinstance(sla_configuration_entry, dict) else {}

        for interface_name, metrics in members.items():
            if not isinstance(metrics, dict):
                continue

            status = metrics.get("status", "unknown")
            latency = float(metrics.get("latency", 0.0))
            jitter = float(metrics.get("jitter", 0.0))
            packet_loss = float(metrics.get("packet_loss", 0.0))
            packet_sent = float(metrics.get("packet_sent", 0.0))
            packet_received = float(metrics.get("packet_received", 0.0))
            sla_targets_met = metrics.get("sla_targets_met", [])
            session = float(metrics.get("session", 0.0))
            tx_bandwidth = float(metrics.get("tx_bandwidth", 0.0))
            rx_bandwidth = float(metrics.get("rx_bandwidth", 0.0))
            state_changed = float(metrics.get("state_changed", 0.0))

            health_check_record = {
                "name": health_check_name,
                "interface": interface_name,
                "vdom": response_vdom,
                "serial": serial_no,
                "status": status,
                "latency": latency,
                "jitter": jitter,
                "packet_loss": packet_loss,
                "packet_sent": packet_sent,
                "packet_received": packet_received,
                "sla_targets_met": sla_targets_met,
                "session": session,
                "tx_bandwidth": tx_bandwidth,
                "rx_bandwidth": rx_bandwidth,
                "state_changed": state_changed,
            }

            if not sla_targets_met:
                health_check_record["sla_target"] = "none"
                health_check_record["latency_threshold"] = 0.0
                health_check_record["jitter_threshold"] = 0.0
                health_check_record["packetloss_threshold"] = 0.0

            health_checks.append(health_check_record)

            if not sla_targets_met:
                point = (
                    Point("fortigate_virtual_wan_health_check")
                    .tag("serial_no", serial_no)
                    .tag("vdom", response_vdom)
                    .tag("health_check_name", health_check_name)
                    .tag("interface_name", interface_name)
                    .tag("status", status)
                    .tag("sla_target", "none")
                    .field("latency", latency)
                    .field("jitter", jitter)
                    .field("packet_loss", packet_loss)
                    .field("packet_sent", packet_sent)
                    .field("packet_received", packet_received)
                    .field("sla_targets_met_count", 0)
                    .field("session", session)
                    .field("tx_bandwidth", tx_bandwidth)
                    .field("rx_bandwidth", rx_bandwidth)
                    .field("state_changed", state_changed)
                    .field("latency_threshold", 0.0)
                    .field("jitter_threshold", 0.0)
                    .field("packetloss_threshold", 0.0)
                )
                points.append(point)

            for target in sla_targets_met:
                if not isinstance(target, int):
                    continue

                latency_threshold = float(sla_thresholds.get(target, {}).get("latency-threshold", 0.0)) if isinstance(sla_thresholds, dict) else 0.0
                jitter_threshold = float(sla_thresholds.get(target, {}).get("jitter-threshold", 0.0)) if isinstance(sla_thresholds, dict) else 0.0
                packetloss_threshold = float(sla_thresholds.get(target, {}).get("packetloss-threshold", 0.0)) if isinstance(sla_thresholds, dict) else 0.0

                health_check_record["sla_target"] = target
                health_check_record["latency_threshold"] = latency_threshold
                health_check_record["jitter_threshold"] = jitter_threshold
                health_check_record["packetloss_threshold"] = packetloss_threshold

                point = (
                    Point("fortigate_virtual_wan_health_check")
                    .tag("serial_no", serial_no)
                    .tag("vdom", response_vdom)
                    .tag("health_check_name", health_check_name)
                    .tag("interface_name", interface_name)
                    .tag("status", status)
                    .tag("sla_target", str(target))
                    .field("latency", latency)
                    .field("jitter", jitter)
                    .field("packet_loss", packet_loss)
                    .field("packet_sent", packet_sent)
                    .field("packet_received", packet_received)
                    .field("sla_targets_met_count", len(sla_targets_met) if isinstance(sla_targets_met, list) else 0)
                    .field("session", session)
                    .field("tx_bandwidth", tx_bandwidth)
                    .field("rx_bandwidth", rx_bandwidth)
                    .field("state_changed", state_changed)
                    .field("latency_threshold", latency_threshold)
                    .field("jitter_threshold", jitter_threshold)
                    .field("packetloss_threshold", packetloss_threshold)
                )
                points.append(point)

    return health_checks, points


def fortiview_realtime_statistics(
    api_client: FortigateClient,
    vdom: str,
    list_of_maximum_bandwidth: dict[str, int],
    sort_by: str = "bandwidth",
    ip_version: str = "ipv4",
    count: int = 100,
    report_by: str | None = None,
) -> tuple[list[dict[str, Any]], list[Point]]:
    """
    Fetches FortiView realtime statistics from the Fortigate API.

    Args:
        api_client (FortigateClient): An instance of the FortigateClient to interact with the API.
        vdom (str): The VDOM for which to fetch FortiView realtime statistics.
        sort_by (str): The field used to sort realtime statistics.
        ip_version (str): The IP version filter.
        count (int): The maximum number of realtime statistic records to fetch.
        report_by (str | None): Optional FortiView report grouping.

    Returns:
        tuple[list[dict[str, Any]], list[Point]]: A tuple containing FortiView realtime statistic dictionaries and InfluxDB points.
    """
    params: dict[str, str | int] = {
        "sort_by": sort_by,
        "ip_version": ip_version,
        "count": count,
        "vdom": vdom,
    }
    if report_by:
        params["report_by"] = report_by

    res = api_client.get(
        "/api/v2/monitor/fortiview/realtime-statistics",
        params=params,
        verify=False # Disable SSL verification for self-signed certificates (not recommended for production use)
    )
    res_json = res.json()

    if not res.ok or "results" not in res_json:
        raise Exception(f"Failed to fetch FortiView realtime statistics: {res.text}")

    results = res_json.get("results", {})
    details = results.get("details", []) if isinstance(results, dict) else []
    response_vdom = res_json.get("vdom", vdom)
    serial_no = res_json.get("serial", "unknown")
    statistics: list[dict[str, Any]] = []
    points: list[Point] = []

    for detail_index, detail in enumerate(details):
        if not isinstance(detail, dict):
            continue

        sessions = detail.get("sessions", 0)
        policy_ipver = detail.get("policy_ipver", "")
        dst_port = detail.get("dst_port", 0)
        protocol = detail.get("protocol", 0)
        srcintf = detail.get("srcintf", "")
        dstintf = detail.get("dstintf", "")
        apps = detail.get("apps", [])
        shaper = detail.get("shaper", "unknown")
        sentbyte = detail.get("sentbyte", 0)
        rcvdbyte = detail.get("rcvdbyte", 0)
        tx_packets = detail.get("tx_packets", 0)
        rx_packets = detail.get("rx_packets", 0)
        tx_shaper_drops = detail.get("tx_shaper_drops", 0)
        rx_shaper_drops = detail.get("rx_shaper_drops", 0)

        # convert to kilobits per second (kbps) if the value is in bits per second (bps)
        tx_bandwidth_bps = detail.get("tx_bandwidth", 0.0)
        tx_bandwidth_kbps = tx_bandwidth_bps / 1000 if tx_bandwidth_bps > 1000 else float(tx_bandwidth_bps)
        rx_bandwidth_bps = detail.get("rx_bandwidth", 0.0)
        rx_bandwidth_kbps = rx_bandwidth_bps / 1000 if rx_bandwidth_bps > 1000 else float(rx_bandwidth_bps)

        # Calculate total bandwidth, maximum bandwidth, and bandwidth utilization, and check if bandwidth is exceeded
        total_bandwidth_kbps = tx_bandwidth_kbps + rx_bandwidth_kbps
        maximum_bandwidth_kbps = list_of_maximum_bandwidth.get(shaper, 0.0)
        bandwidth_utilization_percent = (
            (total_bandwidth_kbps / maximum_bandwidth_kbps * 100) if maximum_bandwidth_kbps else 0.0
        )
        is_bandwidth_exceeded = int(total_bandwidth_kbps > maximum_bandwidth_kbps) if maximum_bandwidth_kbps else 0
        exceed_bandwidth_kbps = total_bandwidth_kbps - maximum_bandwidth_kbps if is_bandwidth_exceeded else 0.0

        statistics.append(
            {
                "detail_index": detail_index,
                "vdom": response_vdom,
                "serial": serial_no,
                "sessions": sessions,
                "policy_ipver": policy_ipver,
                "dst_port": dst_port,
                "protocol": protocol,
                "srcintf": srcintf,
                "dstintf": dstintf,
                "apps": apps,
                "shaper": shaper,
                "sentbyte": sentbyte,
                "rcvdbyte": rcvdbyte,
                "tx_packets": tx_packets,
                "rx_packets": rx_packets,
                "tx_shaper_drops": tx_shaper_drops,
                "rx_shaper_drops": rx_shaper_drops,
                "tx_bandwidth": tx_bandwidth_kbps,
                "rx_bandwidth": rx_bandwidth_kbps,
                "total_bandwidth": total_bandwidth_kbps,
                "maximum_bandwidth": maximum_bandwidth_kbps,
                "bandwidth_utilization_percent": bandwidth_utilization_percent,
                "is_bandwidth_exceeded": is_bandwidth_exceeded,
                "exceed_bandwidth": exceed_bandwidth_kbps,
            }
        )

        point = (
            Point("fortigate_fortiview_realtime_statistics")
            .tag("serial_no", serial_no)
            .tag("vdom", response_vdom)
            .tag("detail_index", str(detail_index))
            .tag("policy_ipver", policy_ipver or "unknown")
            .tag("srcintf", srcintf or "unknown")
            .tag("dstintf", dstintf or "unknown")
            .tag("shaper", shaper or "unknown")
            .field("sessions", sessions)
            .field("dst_port", dst_port)
            .field("protocol", protocol)
            .field("sentbyte", sentbyte)
            .field("rcvdbyte", rcvdbyte)
            .field("tx_packets", tx_packets)
            .field("rx_packets", rx_packets)
            .field("tx_shaper_drops", tx_shaper_drops)
            .field("rx_shaper_drops", rx_shaper_drops)
            .field("tx_bandwidth", tx_bandwidth_kbps)
            .field("rx_bandwidth", rx_bandwidth_kbps)
            .field("total_bandwidth", total_bandwidth_kbps)
            .field("maximum_bandwidth", maximum_bandwidth_kbps)
            .field("bandwidth_utilization_percent", bandwidth_utilization_percent)
            .field("is_bandwidth_exceeded", is_bandwidth_exceeded)
            .field("exceed_bandwidth", exceed_bandwidth_kbps)
            .field("app_count", len(apps) if isinstance(apps, list) else 0)
        )
        points.append(point)

        if not isinstance(apps, list):
            continue

        for app in apps:
            if not isinstance(app, dict):
                continue

            app_id = app.get("id", 0)
            app_point = (
                Point("fortigate_fortiview_realtime_app")
                .tag("serial_no", serial_no)
                .tag("vdom", response_vdom)
                .tag("detail_index", str(detail_index))
                .tag("app_id", str(app_id))
                .tag("app_name", app.get("name", "unknown"))
                .tag("protocol_str", app.get("protocol_str", "unknown"))
                .field("count", app.get("count", 0))
                .field("protocol", app.get("protocol", 0))
                .field("port", app.get("port", 0))
            )
            points.append(app_point)

    return statistics, points


def router_ipv4(api_client: FortigateClient, vdom: str) -> tuple[list[dict[str, Any]], list[Point]]:
    """
    Fetches IPv4 routing table information from the Fortigate API.

    Args:
        api_client (FortigateClient): An instance of the FortigateClient to interact with the API.
        vdom (str): The VDOM for which to fetch the IPv4 routing table information.

    Returns:
        tuple[list[dict[str, Any]], list[Point]]: A tuple containing IPv4 route dictionaries and InfluxDB points.
    """
    res = api_client.get(
        "/api/v2/monitor/router/ipv4",
        params={"vdom": vdom},
        verify=False # Disable SSL verification for self-signed certificates (not recommended for production use)
    )
    res_json = res.json()

    if not res.ok or "results" not in res_json:
        raise Exception(f"Failed to fetch IPv4 routing table information: {res.text}")

    results = res_json.get("results", [])
    response_vdom = res_json.get("vdom", vdom)
    serial_no = res_json.get("serial", "unknown")
    routes: list[dict[str, Any]] = []
    points: list[Point] = []

    for route in results:
        if not isinstance(route, dict):
            continue

        ip_version = route.get("ip_version", 4)
        route_type = route.get("type", "")
        origin = route.get("origin", "")
        ip_mask = route.get("ip_mask", "")
        distance = route.get("distance", 0)
        metric = route.get("metric", 0)
        priority = route.get("priority", 0)
        vrf = route.get("vrf", 0)
        gateway = route.get("gateway", "")
        non_rc_gateway = route.get("non_rc_gateway", "")
        interface = route.get("interface", "")

        routes.append({
            "vdom": response_vdom,
            "serial": serial_no,
            "ip_version": ip_version,
            "type": route_type,
            "origin": origin,
            "ip_mask": ip_mask,
            "distance": distance,
            "metric": metric,
            "priority": priority,
            "vrf": vrf,
            "gateway": gateway,
            "non_rc_gateway": non_rc_gateway,
            "interface": interface,
        })

        point = (
            Point("fortigate_router_ipv4")
            .tag("serial_no", serial_no)
            .tag("vdom", response_vdom)
            .tag("ip_mask", ip_mask or "unknown")
            .tag("interface", interface or "unknown")
            .tag("gateway", gateway or "unknown")
            .tag("type", route_type or "unknown")
            .tag("origin", origin or "unknown")
            .field("ip_version", ip_version)
            .field("distance", distance)
            .field("metric", metric)
            .field("priority", priority)
            .field("vrf", vrf)
        )
        points.append(point)

    return routes, points


def vwan_interface_log(api_client: FortigateClient, vdom: str) -> tuple[list[dict[str, Any]], list[Point]]:
    """
    Fetches virtual WAN interface log information from the Fortigate API.

    Args:
        api_client (FortigateClient): An instance of the FortigateClient to interact with the API.
        vdom (str): The VDOM for which to fetch virtual WAN interface logs.

    Returns:
        tuple[list[dict[str, Any]], list[Point]]: A tuple containing a list of interface log dictionaries and a list of InfluxDB points.
    """
    res = api_client.get(
        "/api/v2/monitor/virtual-wan/interface-log",
        params={"vdom": vdom},
        verify=False # Disable SSL verification for self-signed certificates (not recommended for production use)
    )
    res_json = res.json()

    if not res.ok or "results" not in res_json:
        raise Exception(f"Failed to fetch virtual WAN interface log information: {res.text}")

    results = res_json.get("results", [])
    response_vdom = res_json.get("vdom", vdom)
    serial_no = res_json.get("serial", "unknown")
    interface_logs: list[dict[str, Any]] = []
    points: list[Point] = []

    for interface_entry in results:
        if not isinstance(interface_entry, dict):
            continue

        interface_name = interface_entry.get("interface", "unknown")
        logs = interface_entry.get("logs", [])

        for log_index, log in enumerate(logs):
            if not isinstance(log, dict):
                continue

            timestamp = log.get("timestamp", 0)
            tx_bandwidth = log.get("tx_bandwidth", 0)
            rx_bandwidth = log.get("rx_bandwidth", 0)
            bi_bandwidth = log.get("bi_bandwidth", 0)
            tx_bytes = log.get("tx_bytes", 0)
            rx_bytes = log.get("rx_bytes", 0)
            egress_queue = log.get("egress_queue", [])

            interface_log = {
                "interface": interface_name,
                "vdom": response_vdom,
                "serial": serial_no,
                "timestamp": timestamp,
                "tx_bandwidth": tx_bandwidth,
                "rx_bandwidth": rx_bandwidth,
                "bi_bandwidth": bi_bandwidth,
                "tx_bytes": tx_bytes,
                "rx_bytes": rx_bytes
            }
            interface_logs.append(interface_log)

            point = (
                Point("fortigate_vwan_interface_log")
                .tag("serial_no", serial_no)
                .tag("vdom", response_vdom)
                .tag("interface", interface_name or "unknown")
                .tag("log_index", str(log_index))
                .field("tx_bandwidth", tx_bandwidth)
                .field("rx_bandwidth", rx_bandwidth)
                .field("bi_bandwidth", bi_bandwidth)
                .field("tx_bytes", tx_bytes)
                .field("rx_bytes", rx_bytes)
                .time(timestamp, WritePrecision.S)
            )
            points.append(point)

    return interface_logs, points


def vwan_sla_logs(
    api_client: FortigateClient, 
    vdom: str, 
    sla_configuration: dict[str, Any]
) -> tuple[list[dict[str, Any]], list[Point]]:
    """
    Fetches virtual WAN SLA log information from the Fortigate API.

    Args:
        api_client (FortigateClient): An instance of the FortigateClient to interact with the API.
        vdom (str): The VDOM for which to fetch virtual WAN SLA logs.
        sla_configuration (dict[str, Any]): The SLA configuration dictionary.

    Returns:
        tuple[list[dict[str, Any]], list[Point]]: A tuple containing a list of SLA log dictionaries and a list of InfluxDB points.
    """
    res = api_client.get(
        "/api/v2/monitor/virtual-wan/sla-log",
        params={
            "vdom": vdom, 
            "latest": True, 
            "skip_vpn_child": True, 
            "include_sla_targets_met": True
        },
        verify=False # Disable SSL verification for self-signed certificates (not recommended for production use)
    )
    res_json = res.json()

    if not res.ok or "results" not in res_json:
        raise Exception(f"Failed to fetch virtual WAN SLA log information: {res.text}")

    results = res_json.get("results", [])
    response_vdom = res_json.get("vdom", vdom)
    serial_no = res_json.get("serial", "unknown")
    sla_logs: list[dict[str, Any]] = []
    points: list[Point] = []

    for sla_entry in results:
        if not isinstance(sla_entry, dict):
            continue

        sla_name = sla_entry.get("name", "unknown")
        sla_configuration_entry = sla_configuration.get(sla_name, {})
        interface_name = sla_entry.get("interface", "unknown")
        sla_protocol = sla_configuration_entry.get("protocol", "") if isinstance(sla_configuration_entry, dict) else ""
        sla_servers = sla_configuration_entry.get("server", "") if isinstance(sla_configuration_entry, dict) else ""
        sla_thresholds = sla_configuration_entry.get("sla_thresholds", {}) if isinstance(sla_configuration_entry, dict) else {}
        logs = sla_entry.get("logs", [])

        if not logs:
            point = (
                Point("fortigate_vwan_sla_log")
                .tag("serial_no", serial_no)
                .tag("vdom", response_vdom)
                .tag("sla_name", sla_name or "unknown")
                .tag("sla_target", "none")
                .tag("interface", interface_name or "unknown")
                .tag("link", "no-members")
                .tag("protocol", sla_protocol or "unknown")
                .tag("detected_server", sla_servers if isinstance(sla_servers, str) else "unknown")
                .field("latency", 0.0)
                .field("jitter", 0.0)
                .field("packetloss", 0.0)
                .field("latency_threshold", 0.0)
                .field("jitter_threshold", 0.0)
                .field("packetloss_threshold", 0.0)
            )
            points.append(point)

        for log_index, log in enumerate(logs):
            if not isinstance(log, dict):
                continue

            timestamp = log.get("timestamp", 0)
            link = log.get("link", "unknown")
            latency = float(log.get("latency", 0.0))
            jitter = float(log.get("jitter", 0.0))
            packetloss = float(log.get("packetloss", 0.0))
            sla_targets_met = log.get("sla_targets_met", [])
            
            log_record = {
                "name": sla_name,
                "interface": interface_name,
                "protocol": sla_protocol,
                "vdom": response_vdom,
                "serial": serial_no,
                "timestamp": timestamp,
                "link": link,
                "latency": latency,
                "jitter": jitter,
                "packetloss": packetloss,
            }
            
            if not sla_targets_met:
                log_record["sla_target"] = "none"
                log_record["latency_threshold"] = 0.0
                log_record["jitter_threshold"] = 0.0
                log_record["packetloss_threshold"] = 0.0

            sla_logs.append(log_record)

            for target in sla_targets_met:
                if not isinstance(target, int):
                    continue
                
                latency_threshold = float(sla_thresholds.get(target, {}).get("latency-threshold", 0)) if isinstance(sla_thresholds, dict) else 0.0
                jitter_threshold = float(sla_thresholds.get(target, {}).get("jitter-threshold", 0)) if isinstance(sla_thresholds, dict) else 0.0
                packetloss_threshold = float(sla_thresholds.get(target, {}).get("packetloss-threshold", 0)) if isinstance(sla_thresholds, dict) else 0.0
                
                log_record["sla_target"] = target
                log_record["latency_threshold"] = latency_threshold
                log_record["jitter_threshold"] = jitter_threshold
                log_record["packetloss_threshold"] = packetloss_threshold
                
                point = (
                    Point("fortigate_vwan_sla_log")
                    .tag("serial_no", serial_no)
                    .tag("vdom", response_vdom)
                    .tag("sla_name", sla_name or "unknown")
                    .tag("sla_target", str(target))
                    .tag("interface", interface_name or "unknown")
                    .tag("link", link or "unknown")
                    .tag("log_index", str(log_index))
                    .tag("protocol", sla_protocol or "unknown")
                    .tag("detected_server", sla_servers if isinstance(sla_servers, str) else "unknown")
                    .field("latency", latency)
                    .field("jitter", jitter)
                    .field("packetloss", packetloss)
                    .field("latency_threshold", latency_threshold)
                    .field("jitter_threshold", jitter_threshold)
                    .field("packetloss_threshold", packetloss_threshold)
                    .time(timestamp, WritePrecision.S)
                )
                points.append(point)

    return sla_logs, points


def traffic_history_interface(
    api_client: FortigateClient, 
    vdom: str, interface_name: str, 
    interface_alias: str
) -> tuple[list[dict[str, Any]], list[Point]]:
    """
    Fetches system traffic history information for an interface from the Fortigate API.

    Args:
        api_client (FortigateClient): An instance of the FortigateClient to interact with the API.
        vdom (str): The VDOM for which to fetch traffic history.
        interface_name (str): The interface name used for tagging traffic history points.
        interface_alias (str): The interface alias used for tagging traffic history points.

    Returns:
        tuple[list[dict[str, Any]], list[Point]]: A tuple containing a list of traffic history dictionaries and a list of InfluxDB points.
    """
    res = api_client.get(
        "/api/v2/monitor/system/traffic-history/interface",
        params={"vdom": vdom, "interface": interface_name, "time_period": "hour"},
        verify=False # Disable SSL verification for self-signed certificates (not recommended for production use)
    )
    res_json = res.json()

    if not res.ok or "results" not in res_json:
        raise Exception(f"Failed to fetch traffic history: {res.text}")

    results = res_json.get("results", {})
    response_vdom = res_json.get("vdom", vdom)
    serial_no = res_json.get("serial", "unknown")
    traffic_history: list[dict[str, Any]] = []
    points: list[Point] = []

    # Extract last TX/RX values
    last_tx = results.get("last_tx", 0)
    last_rx = results.get("last_rx", 0)

    # Parse TX history
    tx_logs = results.get("tx", [])
    for tx_index, tx_entry in enumerate(tx_logs):
        if not isinstance(tx_entry, dict):
            continue

        utc_ms = tx_entry.get("utc_ms", 0)
        bps = tx_entry.get("bps", 0)

        traffic_record = {
            "interface": interface_name,
            "interface_alias": interface_alias,
            "vdom": response_vdom,
            "serial": serial_no,
            "direction": "tx",
            "utc_ms": utc_ms,
            "bps": bps,
        }
        traffic_history.append(traffic_record)

        point = (
            Point("fortigate_traffic_history_tx")
            .tag("serial_no", serial_no)
            .tag("vdom", response_vdom)
            .tag("interface", interface_name or "unknown")
            .tag("interface_alias", interface_alias or "unknown")
            .tag("direction", "tx")
            .field("bps", bps)
            .time(utc_ms, WritePrecision.MS)  # Convert milliseconds to nanoseconds
        )
        points.append(point)

    # Parse RX history
    rx_logs = results.get("rx", [])
    for rx_index, rx_entry in enumerate(rx_logs):
        if not isinstance(rx_entry, dict):
            continue

        utc_ms = rx_entry.get("utc_ms", 0)
        bps = rx_entry.get("bps", 0)
        traffic_record = {
            "interface": interface_name,
            "interface_alias": interface_alias,
            "vdom": response_vdom,
            "serial": serial_no,
            "direction": "rx",
            "utc_ms": utc_ms,
            "bps": bps,
        }
        traffic_history.append(traffic_record)

        point = (
            Point("fortigate_traffic_history_rx")
            .tag("serial_no", serial_no)
            .tag("vdom", response_vdom)
            .tag("interface", interface_name or "unknown")
            .tag("interface_alias", interface_alias or "unknown")
            .tag("direction", "rx")
            .field("bps", bps)
            .time(utc_ms, WritePrecision.MS)  # Convert milliseconds to nanoseconds
        )
        points.append(point)

    # Add summary point with last TX/RX values
    summary_point = (
        Point("fortigate_traffic_history_summary")
        .tag("serial_no", serial_no)
        .tag("vdom", response_vdom)
        .tag("interface", interface_name or "unknown")
        .tag("interface_alias", interface_alias or "unknown")
        .field("last_tx_bps", last_tx)
        .field("last_rx_bps", last_rx)
    )
    points.append(summary_point)

    return traffic_history, points

def historical_statistics(
    api_client: FortigateClient,
    vdom: str,
    report_by: str = "application",
    sort_by: str = "bytes",
    ip_version: str = "ipv4",
    count: int = 100,
    device: str = "disk",
    latest: bool = False,
    filter_params: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], list[Point]]:
    """
    Fetches FortiView historical statistics from the Fortigate API.

    Args:
        api_client (FortigateClient): An instance of the FortigateClient to interact with the API.
        vdom (str): The VDOM for which to fetch historical statistics.
        report_by (str): The field used to group historical statistics (e.g. application, category).
        sort_by (str): The field used to sort historical statistics.
        ip_version (str): The IP version filter.
        count (int): The maximum number of historical statistic records to fetch.
        device (str): The device for log storage (disk or memory).
        start (int): The start Unix timestamp for historical statistics.
        end (int): The end Unix timestamp for historical statistics.
        latest (bool): Whether to fetch the latest historical statistics.
        filter_params (dict[str, Any] | None): Optional filter parameters serialized as JSON.

    Returns:
        tuple[list[dict[str, Any]], list[Point]]: A tuple containing historical statistic dictionaries and InfluxDB points.
    """
    params: dict[str, Any] = {
        "report_by": report_by,
        "sort_by": sort_by,
        "ip_version": ip_version,
        "count": count,
        "device": device,
        "latest": str(latest).lower(),
        "vdom": vdom,
    }
    if filter_params:
        params["filter"] = json.dumps(filter_params)

    res = api_client.get(
        "/api/v2/monitor/fortiview/historical-statistics",
        params=params,
        verify=False,  # Disable SSL verification for self-signed certificates (not recommended for production use)
    )
    res_json = res.json()

    if not res.ok or "results" not in res_json:
        raise Exception(f"Failed to fetch FortiView historical statistics: {res.text}")

    results = res_json.get("results", {})
    details = results.get("details", []) if isinstance(results, dict) else []
    response_vdom = res_json.get("vdom", vdom)
    serial_no = res_json.get("serial", "unknown")
    statistics: list[dict[str, Any]] = []
    points: list[Point] = []

    app_name_cache: dict[int, str] = {}

    def resolve_app_name(app_id: int) -> str:
        if app_id in app_name_cache:
            return app_name_cache[app_id]
        lookup_res = api_client.get(
            "/api/v2/cmdb/application/name",
            params={"filter": f"id=={app_id}", "format": "name", "vdom": vdom},
            verify=False,
        )
        if lookup_res.ok:
            lookup_json = lookup_res.json()
            lookup_results = lookup_json.get("results", [])
            if lookup_results and isinstance(lookup_results[0], dict):
                name = lookup_results[0].get("name", "")
                if name:
                    app_name_cache[app_id] = name
                    return name
        fallback = f"app_{app_id}"
        app_name_cache[app_id] = fallback
        return fallback

    for detail_index, detail in enumerate(details):
        if not isinstance(detail, dict):
            continue

        apps = detail.get("apps", [])
        sessions = detail.get("sessions", 0)
        session_allow = detail.get("session_allow", 0)
        session_block = detail.get("session_block", 0)
        rcvdbyte = detail.get("rcvdbyte", 0)
        sentbyte = detail.get("sentbyte", 0)
        total_bytes = detail.get("bytes", 0)
        category = detail.get("category", "unknown")
        risk_id = detail.get("risk_id", 0)
        risk_txt = detail.get("risk_txt", "")

        statistics.append(
            {
                "detail_index": detail_index,
                "vdom": response_vdom,
                "serial": serial_no,
                "category": category,
                "risk_id": risk_id,
                "risk_txt": risk_txt,
                "sessions": sessions,
                "session_allow": session_allow,
                "session_block": session_block,
                "rcvdbyte": rcvdbyte,
                "sentbyte": sentbyte,
                "bytes": total_bytes,
            }
        )

        point = (
            Point("fortigate_fortiview_historical_statistics")
            .tag("serial_no", serial_no)
            .tag("vdom", response_vdom)
            .tag("category", category or "unknown")
            .tag("risk_id", str(risk_id))
            .tag("risk_txt", risk_txt or "none")
            .tag("report_by", report_by)
            .field("sessions", sessions)
            .field("session_allow", session_allow)
            .field("session_block", session_block)
            .field("rcvdbyte", rcvdbyte)
            .field("sentbyte", sentbyte)
            .field("bytes", total_bytes)
        )
        points.append(point)

        if not isinstance(apps, list):
            continue

        for app in apps:
            if not isinstance(app, dict):
                continue

            app_id = app.get("id", 0)
            app_name = app.get("name", "")
            if not app_name and app_id:
                app_name = resolve_app_name(app_id)

            app_point = (
                Point("fortigate_fortiview_historical_app_statistics")
                .tag("serial_no", serial_no)
                .tag("vdom", response_vdom)
                .tag("category", category or "unknown")
                .tag("risk_id", str(risk_id))
                .tag("risk_txt", risk_txt or "none")
                .tag("app_id", str(app_id))
                .tag("app_name", app_name or "unknown")
                .field("sessions", app.get("sessions", sessions))
                .field("session_allow", app.get("session_allow", session_allow))
                .field("session_block", app.get("session_block", session_block))
                .field("rcvdbyte", app.get("rcvdbyte", rcvdbyte))
                .field("sentbyte", app.get("sentbyte", sentbyte))
                .field("bytes", app.get("bytes", total_bytes))
            )
            points.append(app_point)

    return statistics, points

import json
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
                "alias": item.get("alias", ""),
                "vdom": item.get("vdom", ""),
                "monitor-bandwith": item.get("monitor-bandwith", "disable")
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

        for item in res_json.get("results", []):
            vdom_list.append(item.get("name", ""))

        if len(vdom_list) >= size or matched_count == 0:
            point = (
                Point("vdom_count")
                .tag("serial_no", res_json.get("serial_no", "unknown"))
                .field("count", len(vdom_list))
                .field("vdoms", json.dumps(vdom_list))
            )
            vdom_history_points.append(point)
            break

        start = next_idx

    return vdom_list, vdom_history_points


def firewall_traffic_shapper(api_client: FortigateClient, vdom: str) -> tuple[list[dict[str, Any]], dict[str, int], list[Point]]:
    """
    Fetches firewall traffic shaper information from the Fortigate API.

    Args:
        api_client (FortigateClient): An instance of the FortigateClient to interact with the API.
        vdom (str): The VDOM for which to fetch the traffic shaper information.
        
    Returns:
        tuple[list[dict[str, Any]], dict[str, int], list[Point]]: A tuple containing a list of dictionaries with firewall traffic shaper information and a list of InfluxDB points.
    """
    traffic_shapers: list[dict[str, Any]] = []
    list_of_maximum_bandwidth: dict[str, int] = {}
    points: list[Point] = []
    start = 0

    while True:
        res = api_client.get(
            "/api/v2/cmdb/firewall.shaper/traffic-shaper",
            params={"vdom": vdom, "start": start, "count": 100},
            verify=False # Disable SSL verification for self-signed certificates (not recommended for production use)
        )
        res_json = res.json()

        if not res.ok or "results" not in res_json:
            raise Exception(f"Failed to fetch firewall traffic shaper information: {res.text}")

        response_vdom = res_json.get("vdom", vdom)
        serial_no = res_json.get("serial", "unknown")
        size = res_json.get("size", 0)
        matched_count = res_json.get("matched_count", 0)
        next_idx = res_json.get("next_idx", 0)

        for item in res_json.get("results", []):
            name = item.get("name", "")
            q_origin_key = item.get("q_origin_key", "")
            guaranteed_bandwidth = item.get("guaranteed-bandwidth", 0)
            maximum_bandwidth = item.get("maximum-bandwidth", 0)
            exceed_bandwidth = item.get("exceed-bandwidth", 0)
            overhead = item.get("overhead", 0)
            q_ref = item.get("q_ref", 0)
            q_static = item.get("q_static", False)
            q_global_entry = item.get("q_global_entry", False)
            q_no_edit = item.get("q_no_edit", False)
            
            list_of_maximum_bandwidth[q_origin_key] = maximum_bandwidth

            traffic_shapers.append({
                "name": name,
                "q_origin_key": q_origin_key,
                "vdom": response_vdom,
                "serial": serial_no,
                "guaranteed-bandwidth": guaranteed_bandwidth,
                "maximum-bandwidth": maximum_bandwidth,
                "bandwidth-unit": item.get("bandwidth-unit", ""),
                "priority": item.get("priority", ""),
                "per-policy": item.get("per-policy", ""),
                "diffserv": item.get("diffserv", ""),
                "diffservcode": item.get("diffservcode", ""),
                "dscp-marking-method": item.get("dscp-marking-method", ""),
                "exceed-bandwidth": exceed_bandwidth,
                "exceed-dscp": item.get("exceed-dscp", ""),
                "maximum-dscp": item.get("maximum-dscp", ""),
                "cos-marking": item.get("cos-marking", ""),
                "cos-marking-method": item.get("cos-marking-method", ""),
                "cos": item.get("cos", ""),
                "exceed-cos": item.get("exceed-cos", ""),
                "maximum-cos": item.get("maximum-cos", ""),
                "overhead": overhead,
                "exceed-class-id": item.get("exceed-class-id", 0),
                "q_ref": q_ref,
                "q_static": q_static,
                "q_no_rename": item.get("q_no_rename", False),
                "q_global_entry": q_global_entry,
                "q_type": item.get("q_type", 0),
                "q_path": item.get("q_path", ""),
                "q_name": item.get("q_name", ""),
                "q_mkey_type": item.get("q_mkey_type", ""),
                "q_no_edit": q_no_edit,
            }) 
            
            point = (
                Point("fortigate_traffic_shaper")
                .tag("serial_no", serial_no)
                .tag("vdom", response_vdom)
                .tag("shaper_name", name or "unknown")
                .tag("priority", item.get("priority", "unknown"))
                .tag("bandwidth_unit", item.get("bandwidth-unit", "unknown"))
                .tag("per_policy", item.get("per-policy", "unknown"))
                .field("guaranteed_bandwidth", guaranteed_bandwidth)
                .field("maximum_bandwidth", maximum_bandwidth)
                .field("exceed_bandwidth", exceed_bandwidth)
                .field("overhead", overhead)
                .field("q_ref", q_ref)
                .field("q_static", 1 if q_static else 0)
                .field("q_global_entry", 1 if q_global_entry else 0)
                .field("q_no_edit", 1 if q_no_edit else 0)
            )
            points.append(point)

        if len(traffic_shapers) >= size or matched_count == 0:
            break

        start = next_idx
    
    return traffic_shapers, list_of_maximum_bandwidth, points


def sdwan_health_check(api_client: FortigateClient, vdom: str) -> tuple[list[dict[str, Any]], list[Point]]:
    """
    Fetches SD-WAN health check information from the Fortigate API.

    Args:
        api_client (FortigateClient): An instance of the FortigateClient to interact with the API.
        vdom (str): The VDOM for which to fetch the SD-WAN health check information.
        
    Returns:
        tuple[list[dict[str, Any]], list[Point]]: A tuple containing a list of dictionaries with SD-WAN health check information and a list of InfluxDB points.
    """
    health_checks: list[dict[str, Any]] = []
    sla_configuration: dict[str, Any] = {}
    points: list[Point] = []
    start = 0

    while True:
        res = api_client.get(
            "/api/v2/cmdb/system/sdwan/health-check",
            params={
                "vdom": vdom, 
                "start": start, 
                "count": 100, 
                "datasource": True, 
                "with_meta": True
            },
            verify=False # Disable SSL verification for self-signed certificates (not recommended for production use)
        )
        res_json = res.json()

        if not res.ok or "results" not in res_json:
            raise Exception(f"Failed to fetch SD-WAN health check information: {res.text}")

        response_vdom = res_json.get("vdom", vdom)
        serial_no = res_json.get("serial", "unknown")
        size = res_json.get("size", 0)
        matched_count = res_json.get("matched_count", 0)
        next_idx = res_json.get("next_idx", 0)

        for item in res_json.get("results", []):
            name = item.get("name", "")
            server = item.get("server", "")
            protocol = item.get("protocol", "")
            port = item.get("port", 0)
            packet_size = item.get("packet-size", 0)
            ha_priority = item.get("ha-priority", 0)
            interval = item.get("interval", 0)
            probe_timeout = item.get("probe-timeout", 0)
            failtime = item.get("failtime", 0)
            recoverytime = item.get("recoverytime", 0)
            probe_count = item.get("probe-count", 0)
            sla_fail_log_period = item.get("sla-fail-log-period", 0)
            sla_pass_log_period = item.get("sla-pass-log-period", 0)
            warning_packetloss = item.get("threshold-warning-packetloss", 0)
            alert_packetloss = item.get("threshold-alert-packetloss", 0)
            warning_latency = item.get("threshold-warning-latency", 0)
            alert_latency = item.get("threshold-alert-latency", 0)
            warning_jitter = item.get("threshold-warning-jitter", 0)
            alert_jitter = item.get("threshold-alert-jitter", 0)
            vrf = item.get("vrf", 0)
            class_id = item.get("class-id", 0)
            members = item.get("members", [])
            sla_entries = item.get("sla", [])

            health_checks.append({
                "name": name,
                "q_origin_key": item.get("q_origin_key", ""),
                "vdom": response_vdom,
                "serial": serial_no,
                "probe-packets": item.get("probe-packets", ""),
                "addr-mode": item.get("addr-mode", ""),
                "system-dns": item.get("system-dns", ""),
                "server": server,
                "detect-mode": item.get("detect-mode", ""),
                "protocol": protocol,
                "port": port,
                "quality-measured-method": item.get("quality-measured-method", ""),
                "security-mode": item.get("security-mode", ""),
                "user": item.get("user", ""),
                "password": item.get("password", ""),
                "packet-size": packet_size,
                "ha-priority": ha_priority,
                "ftp-mode": item.get("ftp-mode", ""),
                "ftp-file": item.get("ftp-file", ""),
                "http-get": item.get("http-get", ""),
                "http-agent": item.get("http-agent", ""),
                "http-match": item.get("http-match", ""),
                "dns-request-domain": item.get("dns-request-domain", ""),
                "dns-match-ip": item.get("dns-match-ip", ""),
                "interval": interval,
                "probe-timeout": probe_timeout,
                "failtime": failtime,
                "recoverytime": recoverytime,
                "probe-count": probe_count,
                "diffservcode": item.get("diffservcode", ""),
                "update-cascade-interface": item.get("update-cascade-interface", ""),
                "update-static-route": item.get("update-static-route", ""),
                "embed-measured-health": item.get("embed-measured-health", ""),
                "sla-id-redistribute": item.get("sla-id-redistribute", 0),
                "sla-fail-log-period": sla_fail_log_period,
                "sla-pass-log-period": sla_pass_log_period,
                "threshold-warning-packetloss": warning_packetloss,
                "threshold-alert-packetloss": alert_packetloss,
                "threshold-warning-latency": warning_latency,
                "threshold-alert-latency": alert_latency,
                "threshold-warning-jitter": warning_jitter,
                "threshold-alert-jitter": alert_jitter,
                "vrf": vrf,
                "source": item.get("source", ""),
                "source6": item.get("source6", ""),
                "members": members,
                "mos-codec": item.get("mos-codec", ""),
                "class-id": class_id,
                "sla": sla_entries,
            })

            point = (
                Point("fortigate_sdwan_health_check")
                .tag("serial_no", serial_no)
                .tag("vdom", response_vdom)
                .tag("health_check_name", name or "unknown")
                .tag("server", server or "unknown")
                .tag("protocol", protocol or "unknown")
                .tag("detect_mode", item.get("detect-mode", "unknown"))
                .tag("quality_measured_method", item.get("quality-measured-method", "unknown"))
                .field("port", port)
                .field("packet_size", packet_size)
                .field("ha_priority", ha_priority)
                .field("interval", interval)
                .field("probe_timeout", probe_timeout)
                .field("failtime", failtime)
                .field("recoverytime", recoverytime)
                .field("probe_count", probe_count)
                .field("sla_fail_log_period", sla_fail_log_period)
                .field("sla_pass_log_period", sla_pass_log_period)
                .field("threshold_warning_packetloss", warning_packetloss)
                .field("threshold_alert_packetloss", alert_packetloss)
                .field("threshold_warning_latency", warning_latency)
                .field("threshold_alert_latency", alert_latency)
                .field("threshold_warning_jitter", warning_jitter)
                .field("threshold_alert_jitter", alert_jitter)
                .field("vrf", vrf)
                .field("class_id", class_id)
                .field("member_count", len(members))
                .field("sla_count", len(sla_entries))
            )
            points.append(point)

            sla_thresholds = {}
            for sla in sla_entries:
                sla_id = sla.get("id", 0)
                
                latency_threshold = sla.get("latency-threshold", 0)
                jitter_threshold = sla.get("jitter-threshold", 0)
                packetloss_threshold = sla.get("packetloss-threshold", 0)
                
                sla_thresholds[sla_id] = {
                    "latency_threshold": latency_threshold,
                    "jitter_threshold": jitter_threshold,
                    "packetloss_threshold": packetloss_threshold,
                }
                
                sla_point = (
                    Point("fortigate_sdwan_health_check_sla")
                    .tag("serial_no", serial_no)
                    .tag("vdom", response_vdom)
                    .tag("sla_name", name or "unknown")
                    .tag("sla_id", str(sla_id))
                    .tag("link_cost_factor", sla.get("link-cost-factor", "unknown"))
                    .field("latency_threshold", latency_threshold)
                    .field("jitter_threshold", jitter_threshold)
                    .field("packetloss_threshold", packetloss_threshold)
                    .field("priority_in_sla", sla.get("priority-in-sla", 0))
                    .field("priority_out_sla", sla.get("priority-out-sla", 0))
                )
                points.append(sla_point)
            
            sla_configuration[name] = {
                "protocol": protocol,
                "server": server,
                "members": members,
                "sla_thresholds": sla_thresholds
            }

        if len(health_checks) >= size or matched_count == 0:
            break

        start = next_idx

    return health_checks, sla_configuration, points


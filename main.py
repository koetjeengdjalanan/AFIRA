"""AFIRA Main Module."""

import asyncio
import logging
import queue
import signal
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from types import FrameType
from typing import Any, Callable, cast

from humanize import precisedelta
from influxdb_client.client.write.point import Point
from rich.console import Console

from config.context import logging_context
from config.db_check import test_db_setup
from config.runtime import initialize_environment
from helper import logging as logging_helper
from helper.db import retrieve_creds, store_points
from lib.data_fetcher import device_data, site_health, wlan_data
from lib.device_details import (
    ap_cpu_util,
    ap_data,
    ap_mem_util,
    ap_power_util,
    ap_radio,
    gateways_hw_data,
    switch_data,
    switch_hw_data,
)
from lib.fortigate.config_fetcher import (
    firewall_traffic_shapper,
    sdwan_health_check,
    system_interface,
    vdoms,
)
from lib.fortigate.info_fetcher import cooperative_security_fabric, firmware, ha_checksum, interface_monitor
from lib.fortigate.log_fetcher import log_disk_event_system
from lib.fortigate.metric_fetcher import (
    fortiview_realtime_statistics,
    historical_statistics,
    license_status,
    system_resource_usage,
    traffic_history_interface,
    vwan_health_check,
    vwan_interface_log,
    vwan_sla_logs,
)
from lib.sites_details import (
    alerts as site_alerts,
)
from lib.sites_details import (
    clients_data,
    device_locations,
    web_app_data,
    wifi_clients_loc,
    wlan_trhougput_trends,
)
from models import EnvironmentsVariables, FortigateClient, HPEOAuth2Client

FetcherItems = list[str] | list[dict[str, Any]] | dict[str, Any]
FetcherResult = tuple[FetcherItems, list[Point]]
FetcherReturn = tuple[FetcherItems | None, list[Point] | None] | None
FetcherFunction = Callable[[HPEOAuth2Client], FetcherReturn]

FortigateFetcherFunction = Callable[[FortigateClient], FetcherResult]
FortigateVdomFetcherFunction = Callable[[FortigateClient, str], FetcherResult]
FortigateDependantVdomFetcherFunction = Callable[[FortigateClient, str, dict[str, Any]], FetcherResult]
FortigateInterfaceFetcherFunction = Callable[[FortigateClient, str, str, str], FetcherResult]
FortigateHAMemberFetcherFunction = Callable[[FortigateClient, str, list[str], int], FetcherResult]

ShutdownSignalHandler = Callable[[int, FrameType | None], None]


def _require_fetcher_result(fetcher: str, result: FetcherReturn) -> FetcherResult:
    """Return a fetcher result or raise when the fetcher returned no data."""
    if result is None:
        raise RuntimeError(f"Fetcher {fetcher} returned None. Aborting AFIRA run.")

    fetcher_items, fetcher_points = result
    if fetcher_items is None:
        raise RuntimeError(f"Fetcher {fetcher} returned None items. Aborting AFIRA run.")
    if fetcher_points is None:
        raise RuntimeError(f"Fetcher {fetcher} returned None points. Aborting AFIRA run.")

    return fetcher_items, fetcher_points


def _has_required_credentials(credentials: dict[str, Any], section: str, required_keys: tuple[str, ...]) -> bool:
    """Return True when a credential section exists and contains all required values."""
    section_credentials = credentials.get(section)
    if not isinstance(section_credentials, dict) or not section_credentials:
        return False

    return all(bool(section_credentials.get(key)) for key in required_keys)


def _run_fortigate_fetcher(
    credentials: dict[str, Any], env_vars: EnvironmentsVariables, logger: logging.Logger
) -> list[Point]:
    """
    Run the Fortigate fetcher and return its results.

    Args:
        credentials (dict[str, Any]): A dictionary containing the credentials for the Fortigate API.
        env_vars (EnvironmentsVariables): The runtime environment configuration.
        logger (logging.Logger): A logger instance for logging.

    Returns:
        list[Point]: A list of InfluxDB points collected from the Fortigate API.
    """
    res_points: list[Point] = []
    res: dict[str, FetcherItems] = {}
    global_fetchers: dict[str, FortigateFetcherFunction] = {
        "vdoms": vdoms,
        "ha_checksum": ha_checksum,
        "system_interface": system_interface,
        "firmware": firmware,
        "license_status": license_status,
        "cooperative_security_fabric": cooperative_security_fabric,
    }
    vdom_specific_fetchers: dict[str, FortigateVdomFetcherFunction] = {
        "firewall_traffic_shapper": firewall_traffic_shapper,
        "sdwan_health_check": sdwan_health_check,
        "historical_statistics": historical_statistics,
        "system_resource_usage": system_resource_usage,
        "vwan_interface_log": vwan_interface_log,
        "interface_monitor": interface_monitor
    }
    dependant_vdom_specific_fetchers: dict[str, FortigateDependantVdomFetcherFunction] = {
        "fortiview_realtime_statistics": fortiview_realtime_statistics,
        "vwan_health_check": vwan_health_check,
        "vwan_sla_logs": vwan_sla_logs,
    }
    interface_specific_fetchers: dict[str, FortigateInterfaceFetcherFunction] = {
        "traffic_history_interface": traffic_history_interface,
    }
    ha_member_specific_fetchers: dict[str, FortigateHAMemberFetcherFunction] = {
        "log_disk_event_system": log_disk_event_system,
    }

    with FortigateClient(**credentials["fortigate"]) as api_client:
        logger.debug("Successfully authenticated with Fortigate API.")

        logger.debug("Start global fetchers loop")
        for fetcher, func in global_fetchers.items():
            try:
                logger.info(f"Running fetcher: {fetcher}")
                fetcher_items, fetcher_points = _require_fetcher_result(fetcher=fetcher, result=func(api_client))
                res.update({fetcher: fetcher_items})
                res_points.extend(fetcher_points)
                logger.debug(f"Fetcher {fetcher} returned {len(fetcher_items)} items and {len(fetcher_points)} points")
            except Exception as e:
                logger.error(f"Fetcher {fetcher} failed with error: {e}. Aborting AFIRA run.")
                raise

        logger.debug("Start VDOM-specific fetchers loop")
        for vdom in cast(list[str], res.get("vdoms", [])):
            vdom_res: dict[str, FetcherItems] = {}

            for fetcher, func in vdom_specific_fetchers.items():
                try:
                    logger.info(f"Running VDOM-specific fetcher: {fetcher} for VDOM: {vdom}")
                    fetcher_items, fetcher_points = _require_fetcher_result(
                        fetcher=fetcher, result=func(api_client, vdom)
                    )
                    vdom_res.update({fetcher: fetcher_items})
                    res_points.extend(fetcher_points)
                    logger.debug(
                        f"VDOM-specific Fetcher {fetcher} for VDOM {vdom} returned {len(fetcher_items)} items and "
                        f"{len(fetcher_points)} points"
                    )
                except Exception as e:
                    logger.warning(
                        f"VDOM-specific Fetcher {fetcher} for VDOM {vdom} failed with error: {e}. Continuing with "
                        "other fetchers."
                    )

            sla_configuration = cast(dict[str, Any], vdom_res.get("sdwan_health_check", []))
            list_of_maximum_bandwidth = cast(dict[str, Any], vdom_res.get("firewall_traffic_shapper", []))

            # Run VDOM-specific fetchers that require data from other VDOM-specific fetchers
            for fetcher, func in dependant_vdom_specific_fetchers.items():
                extra_arg = (
                    sla_configuration
                    if fetcher in ["vwan_health_check", "vwan_sla_logs"]
                    else list_of_maximum_bandwidth
                )

                try:
                    logger.info(f"Running VDOM-specific fetcher: {fetcher} for VDOM: {vdom}")
                    _, fetcher_points = _require_fetcher_result(
                        fetcher=fetcher, result=func(api_client, vdom, extra_arg)
                    )
                    res_points.extend(fetcher_points)
                    logger.debug(f"Fetcher {fetcher} for VDOM {vdom} returned {len(fetcher_points)} points")
                except Exception as e:
                    logger.warning(
                        f"Fetcher {fetcher} for VDOM {vdom} failed with error: {e}. Continuing with other VDOMs."
                    )

        logger.debug("Start HA member-specific fetchers loop")
        for member in cast(list[dict[str, Any]], res.get("ha_checksum", [])):
            serial_no = member.get("serial_no", "unknown")
            vdoms_for_member = member.get("vdoms", [])

            for fetcher, func in ha_member_specific_fetchers.items():
                try:
                    logger.info(
                        f"Running HA member-specific fetcher: {fetcher} for HA member with serial number: {serial_no}"
                    )
                    _, fetcher_points = _require_fetcher_result(
                        fetcher=fetcher,
                        result=func(api_client, serial_no, vdoms_for_member, env_vars.loop_sleep_seconds),
                    )
                    res_points.extend(fetcher_points)
                    logger.debug(
                        f"HA member-specific Fetcher {fetcher} for HA member with serial number {serial_no} "
                        f"returned {len(fetcher_points)} points"
                    )
                except Exception as e:
                    logger.warning(
                        f"HA member-specific Fetcher {fetcher} for HA member with serial number {serial_no}"
                        f" failed with error: {e}. Continuing with other HA members.",
                    )

        logger.debug("Start interface-specific fetchers loop")
        for interface in cast(list[dict[str, Any]], res.get("system_interface", [])):
            interface_name = interface.get("name", "unknown")
            interface_alias = interface.get("alias", "unknown")
            interface_vdom = interface.get("vdom", "unknown")
            is_monitor_bandwidth_enable = True if interface.get("monitor-bandwidth", "disable") == "enable" else False

            if not is_monitor_bandwidth_enable:
                logger.info(
                    f"Skipping interface-specific fetchers for interface: {interface_name} in VDOM: {interface_vdom} "
                    "because bandwidth monitoring is disabled."
                )
                continue

            for fetcher, func in interface_specific_fetchers.items():
                try:
                    logger.info(
                        f"Running interface-specific fetcher: {fetcher} for interface: {interface_name} "
                        f"in VDOM: {interface_vdom}",
                    )
                    _, fetcher_points = _require_fetcher_result(
                        fetcher=fetcher, result=func(api_client, interface_vdom, interface_name, interface_alias)
                    )
                    res_points.extend(fetcher_points)
                    logger.debug(
                        f"Interface-specific Fetcher {fetcher} for interface {interface_name} in "
                        f"VDOM {interface_vdom} returned {len(fetcher_points)} points"
                    )
                except Exception as e:
                    logger.warning(
                        f"Interface-specific Fetcher {fetcher} for interface {interface_name} in "
                        f"VDOM {interface_vdom} failed with error: {e}. Continuing with other interfaces.",
                    )

    return res_points


def _run_aruba_fetcher(credentials: dict[str, Any], logger: logging.Logger) -> list[Point]:
    res_points: list[Point] = []
    res: dict[str, FetcherItems] = {}
    fetcher_func: dict[str, FetcherFunction] = {
        "site_health": site_health,
        "device_data": device_data,
        "wlan_data": wlan_data,
    }
    site_details_func: dict[str, Callable[[HPEOAuth2Client, str], list[Point]]] = {
        "device_locations": device_locations,
        "wifi_clients_loc": wifi_clients_loc,
        "clients_data": clients_data,
        "web_app_data": web_app_data,
        "alerts": site_alerts,
    }
    device_details_func: dict[str, dict[str, Callable[[HPEOAuth2Client, str], list[Point]]]] = {
        "ACCESS_POINT": {
            "ap_data": ap_data,
            "ap_cpu_util": ap_cpu_util,
            "ap_mem_util": ap_mem_util,
            "ap_power_util": ap_power_util,
            "ap_radio": ap_radio,
        },
        "SWITCH": {
            "switch_data": switch_data,
            "switch_hw_data": switch_hw_data,
        },
        "GATEWAY": {
            "gateway_data": gateways_hw_data,
        },
    }

    with HPEOAuth2Client(**credentials["new_central"]) as aruba_api:
        logger.debug("Successfully authenticated with HPE Aruba Central API.")
        logger.debug("Start main Fetchers loop")
        for fetcher, func in fetcher_func.items():
            try:
                logger.info(f"Running fetcher: {fetcher}")
                fetcher_items, fetcher_points = _require_fetcher_result(fetcher=fetcher, result=func(aruba_api))
                res.update({fetcher: fetcher_items})
                res_points.extend(fetcher_points)
                logger.debug(f"Fetcher {fetcher} returned {len(fetcher_items)} items and {len(fetcher_points)} points")
            except Exception as e:
                logger.error(f"Fetcher {fetcher} failed with error: {e}. Aborting AFIRA run.")
                raise
        logger.debug("Start Site Details Fetchers loop")
        for fetcher, func in site_details_func.items():
            for site_id in cast(list[str], res["site_health"]):
                try:
                    logger.info(f"Running site details fetcher: {fetcher}")
                    points = func(aruba_api, site_id)
                    res_points.extend(points)
                    logger.debug(f"Site details fetcher {fetcher} returned {len(points)} points: {points}")
                except Exception as e:
                    logger.warning(
                        f"Site details fetcher {fetcher} failed with error: {e}. Continuing with other fetchers."
                    )
        logger.debug("Start Wlan Details Fetcher loop")
        for wlan in cast(list[str], res["wlan_data"]):
            try:
                logger.info(f"Running WLAN details fetcher for WLAN: {wlan}")
                points = wlan_trhougput_trends(aruba_api, wlan)
                res_points.extend(points)
                logger.debug(f"WLAN details fetcher for {wlan} returned {len(points)} points: {points}")
            except Exception as e:
                logger.warning(f"WLAN details fetcher for {wlan} failed with error: {e}. Continuing with other WLANs.")
        logger.debug("Start Device Hw Details Fetcher loop")
        for device in cast(list[dict[str, Any]], res["device_data"]):
            serial_number = cast(str, device.get("serial_number", "unknown"))
            try:
                device_type = cast(str, device.get("device_type", "unknown"))
                if device_type not in device_details_func:
                    logger.warning(
                        f"Device type {device_type} for device with serial number {serial_number} is not supported. "
                        "Skipping device details fetchers for this device."
                    )
                    continue
                for device_details, func in device_details_func.get(device_type, {}).items():
                    logger.info(
                        f"Running device details fetcher {device_details} "
                        f"for device with serial number: {serial_number}"
                    )
                    points = func(aruba_api, serial_number)
                    res_points.extend(points)
                    logger.debug(
                        f"Device details fetcher {device_details} for device {serial_number} "
                        f"returned {len(points)} points: {points}"
                    )
            except Exception as e:
                logger.warning(
                    f"Device details fetcher for {serial_number} failed with error: {e}. "
                    "Continuing with other devices."
                )

    return res_points


def run_once(env_vars: EnvironmentsVariables) -> int:
    """Run one AFIRA collection cycle and return the number of collected points."""
    logger = logging.getLogger("AFIRA.Main")
    creds: dict[str, Any] = retrieve_creds()
    res_points: list[Point] = []

    test_db_setup(setting=env_vars.influxdb)

    fetcher_tasks: dict[str, Callable[[], list[Point]]] = {}

    if _has_required_credentials(creds, "new_central", ("token_url", "base_url", "client_id", "client_secret")):
        fetcher_tasks["aruba"] = lambda: _run_aruba_fetcher(credentials=creds, logger=logger)
    else:
        logger.warning("Skipping Aruba fetcher because new_central credentials are missing or incomplete.")

    if _has_required_credentials(creds, "fortigate", ("base_url", "api_token")):
        fetcher_tasks["fortigate"] = lambda: _run_fortigate_fetcher(credentials=creds, env_vars=env_vars, logger=logger)
    else:
        logger.warning("Skipping Fortigate fetcher because fortigate credentials are missing or incomplete.")

    if not fetcher_tasks:
        logger.warning("No valid credentials were found. Skipping all fetchers.")
        logger.debug(f"Finished all fetchers. Total points collected: {len(res_points)}")
        return len(res_points)

    with ThreadPoolExecutor(max_workers=len(fetcher_tasks), thread_name_prefix="AFIRA-Fetcher") as executor:
        futures = {executor.submit(task): fetcher_name for fetcher_name, task in fetcher_tasks.items()}

        for future in as_completed(futures):
            fetcher_name = futures[future]
            try:
                fetcher_points = future.result()
                res_points.extend(fetcher_points)
                logger.info(f"Fetcher {fetcher_name} completed with {len(fetcher_points)} points.")
            except Exception:
                logger.exception(f"Fetcher {fetcher_name} failed. Aborting AFIRA run.")
                for pending_future in futures:
                    if pending_future is not future:
                        pending_future.cancel()
                raise

    logger.info("Finished all fetchers. Storing points in InfluxDB...")
    _ = asyncio.run(store_points(points=res_points, influx_conf=env_vars.influxdb, debug_mode=env_vars.debug_mode))

    logger.debug(f"Finished all fetchers. Total points collected: {len(res_points)}")
    return len(res_points)


def run_forever(env_vars: EnvironmentsVariables, shutdown_event: threading.Event) -> None:
    """Run AFIRA collection cycles indefinitely until a shutdown signal is received."""
    logger = logging.getLogger("AFIRA.Main")
    iteration: int = 0

    logger.info(f"AFIRA loop started with {env_vars.loop_sleep_seconds} seconds between iterations.")

    while not shutdown_event.is_set():
        start_time = datetime.now()
        iteration += 1
        try:
            logger.info("Starting AFIRA iteration")
            point_count = run_once(env_vars=env_vars)
            logger.info(
                f"AFIRA iteration completed at {precisedelta(datetime.now() - start_time)} with {point_count} points."
            )
        except Exception:
            logger.exception(
                f"AFIRA iteration {iteration} failed. Sleeping {env_vars.loop_sleep_seconds} seconds before retrying. "
                f"After duration: {precisedelta(datetime.now() - start_time)}."
            )

        if shutdown_event.is_set():
            break

        logger.info(f"AFIRA sleeping {env_vars.loop_sleep_seconds} seconds before the next iteration.")
        shutdown_event.wait(timeout=int(env_vars.loop_sleep_seconds))

    logger.info("AFIRA loop stopped.")


def _build_shutdown_signal_handler(shutdown_event: threading.Event, logger: logging.Logger) -> ShutdownSignalHandler:
    """Create a signal handler that asks the AFIRA loop to stop gracefully."""

    def _handle_shutdown_signal(signum: int, _frame: FrameType | None) -> None:
        try:
            signal_name = signal.Signals(signum).name
        except ValueError:
            signal_name = str(signum)

        logger.info(f"AFIRA received shutdown signal {signal_name}. Exiting after the current iteration.")
        shutdown_event.set()

    return _handle_shutdown_signal


def _register_shutdown_signal_handlers(shutdown_event: threading.Event, logger: logging.Logger) -> None:
    """Register container-friendly SIGTERM and SIGINT handlers."""
    handler = _build_shutdown_signal_handler(shutdown_event=shutdown_event, logger=logger)
    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)


if __name__ == "__main__":
    console: Console = Console()
    console.print("[bold green]Starting AFIRA...[/bold green]")

    env_vars = initialize_environment()
    shutdown_event = threading.Event()

    log_q: queue.Queue[logging.LogRecord] = queue.Queue(maxsize=-1)
    # TODO: Add a healthcheck for container readiness and liveness, and add a log message when the healthcheck is ready.

    with logging_context(
        settings=env_vars.logging,
        console=console,
        log_queue=log_q,
        level=env_vars.log_level,
        influxdb_settings=env_vars.influxdb,
    ):
        logging_helper.worker_logger(log_queue=log_q, log_level=env_vars.log_level)
        log = logging.getLogger("AFIRA")
        _register_shutdown_signal_handlers(shutdown_event=shutdown_event, logger=log)
        log.info("AFIRA is starting")
        try:
            run_forever(env_vars=env_vars, shutdown_event=shutdown_event)
        except Exception as e:
            log.critical(msg=f"AFIRA encountered a critical error: {e}", exc_info=True, stack_info=True)
            raise SystemExit(1) from e
        finally:
            log.info("AFIRA shutdown complete.")

"""AFIRA Main Module."""

import asyncio
import logging
import queue
import signal
import threading
from datetime import datetime
from types import FrameType
from typing import Any, Callable, cast

from humanize import precisedelta
from influxdb_client.client.write.point import Point
from lib.fortigate.log_fetcher import log_disk_event_system
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
    vdoms
)
from lib.fortigate.metric_fetcher import (
    system_resource_usage, 
    license_status, 
    vwan_health_check, 
    fortiview_realtime_statistics, 
    router_ipv4,
    vwan_interface_log,
    vwan_sla_logs,
    traffic_history_interface
)
from lib.fortigate.info_fetcher import ha_checksum, firmware
from lib.sites_details import alerts as site_alerts
from lib.sites_details import clients_data, device_locations, web_app_data, wifi_clients_loc, wlan_trhougput_trends
from models import EnvironmentsVariables, FortigateClient, HPEOAuth2Client

FetcherItems = list[str] | list[dict[str, Any]]
FetcherResult = tuple[FetcherItems, list[Point]]
FetcherReturn = tuple[FetcherItems | None, list[Point] | None] | None
FetcherFunction = Callable[[HPEOAuth2Client], FetcherReturn]
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


def _run_fortigate_fetcher(credentials: dict[str, Any], env_vars: EnvironmentsVariables, logger: logging.Logger) -> list[Point]:
    """
    Run the Fortigate fetcher and return its results.
    
    Flow:
        get_vdom()                          checked v
        get_interface()                     checked v
        get_ha_checksum()                   checked v
        get_firmware()
        get_log_device_state()
        get_cooperative_security_fabric()
       
        for vdom in vdoms:
            # Static data
            get_traffic_shapper()
            get_sdwan_health_check()
           
            # Time series data
            get_resource_usage()
            get_license_status()
            get_fortiview_realtime_statistics()
            get_vwan_health_check()
            get_router_ipv4_routing_table()
       
        for serial_no, vdom in ha_vdoms:
            get_event_log(serial_no, vdom)
       
        for interface, vdom in interfaces:
            get_interface_traffic_history(interface, vdom)
            get_vwan_sla_log(interface, vdom)
            get_vwan_interface_log(interface, vdom)
    
    Args:
        credentials (dict[str, Any]): A dictionary containing the credentials for the Fortigate API.
        logger (logging.Logger): A logger instance for logging.
    
    Returns:
        list[Point]: A list of InfluxDB points collected from the Fortigate API.
    """
    with FortigateClient(**credentials["fortigate"]) as api_client:
        logger.debug("Successfully authenticated with Fortigate API.")
        points: list[Point] = []
        
        try:
            logger.info("Running VDOMs fetcher")
            vdoms_list, vdom_history_points = vdoms(api_client=api_client)
            points.extend(vdom_history_points)
            logger.debug(
                "VDOMs fetcher returned %s VDOMs and %s points", 
                len(vdoms_list), 
                len(vdom_history_points)
            )
        except Exception as e:
            logger.error(f"VDOMs fetcher failed with error: {e}. Aborting AFIRA run.")
            raise
        
        try:
            logger.info("Running HA checksum fetcher")
            ha_members, ha_sync_state = ha_checksum(api_client=api_client)
            points.extend(ha_sync_state)
            logger.debug(
                "HA checksum fetcher returned %s HA members and %s points", 
                len(ha_members), 
                len(ha_sync_state)
            )
        except Exception as e:
            logger.error(f"HA checksum fetcher failed with error: {e}. Aborting AFIRA run.")
            raise
        
        try:
            logger.info("Running interfaces fetcher")
            interfaces, interface_status = system_interface(api_client=api_client)
            points.extend(interface_status)
            logger.debug(
                "Interfaces fetcher returned %s interfaces and %s points",
                len(interfaces),
                len(interface_status),
            )
        except Exception as e:
            logger.error(f"Interfaces fetcher failed with error: {e}. Aborting AFIRA run.")
            raise
        
        try:
            logger.info("Running firmware fetcher")
            _, firmware_update_available, firmware_update_history = firmware(api_client=api_client)
            points.extend(firmware_update_available)
            points.extend(firmware_update_history)
            logger.debug(
                "Firmware fetcher returned %s points for firmware update availability and %s points for firmware update history",
                len(firmware_update_available),
                len(firmware_update_history),
            )
        except Exception as e:
            logger.error(f"Firmware fetcher failed with error: {e}. Aborting AFIRA run.")
            raise

        # License Status
        try:
            logger.info(f"Running license status fetcher")
            _, license_points = license_status(
                api_client=api_client
            )
            points.extend(license_points)
            logger.debug(
                "License status fetcher returned %s points",
                len(license_points),
            )
        except Exception as e:
            logger.warning(
                f"License status fetcher failed with error: {e}. "
                "Continuing with other fetchers."
            )
           
        
        # Fetch per-VDOM data
        logger.debug("Start VDOM-specific fetchers loop")
        for vdom in vdoms_list:
            # Firewall Traffic Shaper
            try:
                logger.info(f"Running firewall traffic shaper fetcher for VDOM: {vdom}")
                _, list_of_maximum_bandwith, traffic_shapper_points = firewall_traffic_shapper(
                    api_client=api_client,
                    vdom=vdom,
                )
                points.extend(traffic_shapper_points)
                logger.debug(
                    "Firewall traffic shaper fetcher for VDOM %s returned %s points",
                    vdom,
                    len(traffic_shapper_points),
                )
            except Exception as e:
                logger.warning(
                    f"Firewall traffic shaper fetcher for VDOM {vdom} failed with error: {e}. "
                    "Continuing with other VDOMs."
                )
            
            # SD-WAN Health Check
            try:
                logger.info(f"Running SD-WAN health check fetcher for VDOM: {vdom}")
                _, sla_configuration, sdwan_points = sdwan_health_check(
                    api_client=api_client,
                    vdom=vdom,
                )
                points.extend(sdwan_points)
                logger.debug(
                    "SD-WAN health check fetcher for VDOM %s returned %s points",
                    vdom,
                    len(sdwan_points),
                )
            except Exception as e:
                logger.warning(
                    f"SD-WAN health check fetcher for VDOM {vdom} failed with error: {e}. "
                    "Continuing with other VDOMs."
                )
            
            # Virtual WAN Health Check
            try:
                logger.info(f"Running virtual WAN health check fetcher for VDOM: {vdom}")
                _, vwan_points = vwan_health_check(
                    api_client=api_client,
                    vdom=vdom,
                    sla_configuration=sla_configuration
                )
                points.extend(vwan_points)
                logger.debug(
                    "Virtual WAN health check fetcher for VDOM %s returned %s points",
                    vdom,
                    len(vwan_points),
                )
            except Exception as e:
                logger.warning(
                    f"Virtual WAN health check fetcher for VDOM {vdom} failed with error: {e}. "
                    "Continuing with other VDOMs."
                )
            
   
            # FortiView Realtime Statistics
            try:
                # need data 
                logger.info(f"Running FortiView realtime statistics fetcher for VDOM: {vdom}")
                _, fortiview_points = fortiview_realtime_statistics(
                    api_client=api_client,
                    vdom=vdom,
                    list_of_maximum_bandwith=list_of_maximum_bandwith
                )
                points.extend(fortiview_points)
                logger.debug(
                    "FortiView realtime statistics fetcher for VDOM %s returned %s points",
                    vdom,
                    len(fortiview_points),
                )
            except Exception as e:
                logger.warning(
                    f"FortiView realtime statistics fetcher for VDOM {vdom} failed with error: {e}. "
                    "Continuing with other VDOMs."
                )
            
            # IPv4 Routing Table
            # try:
            #     logger.info(f"Running IPv4 routing table fetcher for VDOM: {vdom}")
            #     _, router_points = router_ipv4(
            #         api_client=api_client,
            #         vdom=vdom,
            #     )
            #     points.extend(router_points)
            #     logger.debug(
            #         "IPv4 routing table fetcher for VDOM %s returned %s points",
            #         vdom,
            #         len(router_points),
            #     )
            # except Exception as e:
            #     logger.warning(
            #         f"IPv4 routing table fetcher for VDOM {vdom} failed with error: {e}. "
            #         "Continuing with other VDOMs."
            #     )
            
            # System Resource Usage
            try:
                logger.info(f"Running system resource usage fetcher for VDOM: {vdom}")
                _, resource_points = system_resource_usage(
                    api_client=api_client,
                    vdom=vdom,
                )
                points.extend(resource_points)
                logger.debug(
                    "System resource usage fetcher for VDOM %s returned %s points",
                    vdom,
                    len(resource_points),
                )
            except Exception as e:
                logger.warning(
                    f"System resource usage fetcher for VDOM {vdom} failed with error: {e}. "
                    "Continuing with other VDOMs."
                )

            # Virtual WAN Interface Log
            try:
                logger.info(f"Running virtual WAN interface log fetcher for  VDOM: {vdom}")
                _, vwan_iface_points = vwan_interface_log(
                    api_client=api_client,
                    vdom=vdom
                )
                points.extend(vwan_iface_points)
                logger.debug(
                    "Virtual WAN interface log fetcher for  VDOM: %s returned %s points",
                    vdom,
                    len(vwan_iface_points),
                )
            except Exception as e:
                logger.warning(
                    f"Virtual WAN interface log fetcher for  VDOM: {vdom} failed with error: {e}. "
                    "Continuing with other interfaces."
                )
            
            # Virtual WAN SLA Log
            try:
                logger.info(f"Running virtual WAN SLA log fetcher for  VDOM: {vdom}")
                _, vwan_sla_points = vwan_sla_logs(
                    api_client=api_client,
                    vdom=vdom,
                    sla_configuration=sla_configuration
                )
                points.extend(vwan_sla_points)
                logger.debug(
                    "Virtual WAN SLA log fetcher for  VDOM: %s returned %s points",
                    vdom,
                    len(vwan_sla_points),
                )
            except Exception as e:
                logger.warning(
                    f"Virtual WAN SLA log fetcher for  VDOM: {vdom} failed with error: {e}. "
                    "Continuing with other interfaces."
                )

        logger.debug("Start HA members loop")
        for member in ha_members:
            serial_no = member.get("serial_no", "unknown")
            member_vdoms = member.get("vdoms", [])
            
            # Event Log Disk System
            try:
                _, log_points = log_disk_event_system(
                    api_client=api_client, 
                    serial_no=serial_no, 
                    vdoms=member_vdoms, 
                    interval_s=env_vars.loop_sleep_seconds
                )
                points.extend(log_points)
                logger.debug(
                    "Event log disk system fetcher for HA member with serial number %s returned %s points",
                    serial_no,
                    len(log_points),
                )
            except Exception as e:
                logger.warning(
                    f"Event log disk system fetcher for HA member with serial number {serial_no} failed with error: {e}. "
                    "Continuing with other HA members."
                )
        
        # Fetch per-interface data
        logger.debug("Start interface-specific fetchers loop")
        for interface in interfaces:
            interface_name = interface.get("name", "unknown")
            interface_alias = interface.get("alias", "unknown")
            interface_vdom = interface.get("vdom", "unknown")
            is_monitor_bandwidth_enable = True if interface.get("monitor-bandwith", "disable") == "enable" else False
            
            if not is_monitor_bandwidth_enable:
                logger.info(
                    f"Skipping traffic history fetcher for interface: {interface_name} in VDOM: {interface_vdom} "
                    "because bandwidth monitoring is disabled."
                )
                continue

            # Traffic History Interface
            try:
                logger.info(f"Running traffic history fetcher for interface: {interface_name} in VDOM: {interface_vdom}")
                _, traffic_hist_points = traffic_history_interface(
                    api_client=api_client,
                    vdom=interface_vdom,
                    interface_name=interface_name,
                    interface_alias=interface_alias
                )
                points.extend(traffic_hist_points)
                logger.debug(
                    "Traffic history fetcher for interface %s returned %s points",
                    interface_name,
                    len(traffic_hist_points)
                )
            except Exception as e:
                logger.warning(
                    f"Traffic history fetcher for interface {interface_name} failed with error: {e}. "
                    "Continuing with other interfaces."
                )
                   
    return points
    

def run_once(env_vars: EnvironmentsVariables) -> int:
    """Run one AFIRA collection cycle and return the number of collected points."""
    logger = logging.getLogger("AFIRA.Main")
    creds: dict[str, Any] = retrieve_creds()
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

    test_db_setup(setting=env_vars.influxdb)

    # with HPEOAuth2Client(**creds["new_central"]) as aruba_api:
    #     logger.debug("Successfully authenticated with HPE Aruba Central API.")
    #     logger.debug("Start main Fetchers loop")
    #     for fetcher, func in fetcher_func.items():
    #         try:
    #             logger.info(f"Running fetcher: {fetcher}")
    #             fetcher_items, fetcher_points = _require_fetcher_result(fetcher=fetcher, result=func(aruba_api))
    #             res.update({fetcher: fetcher_items})
    #             res_points.extend(fetcher_points)
    #             logger.debug(f"Fetcher {fetcher} returned {len(fetcher_items)} items and {len(fetcher_points)} points")
    #         except Exception as e:
    #             logger.error(f"Fetcher {fetcher} failed with error: {e}. Aborting AFIRA run.")
    #             raise
    #     logger.debug("Start Site Details Fetchers loop")
    #     for fetcher, func in site_details_func.items():
    #         for site_id in cast(list[str], res["site_health"]):
    #             try:
    #                 logger.info(f"Running site details fetcher: {fetcher}")
    #                 points = func(aruba_api, site_id)
    #                 res_points.extend(points)
    #                 logger.debug("Site details fetcher %s returned %s points: %s", fetcher, len(points), points)
    #             except Exception as e:
    #                 logger.warning(
    #                     f"Site details fetcher {fetcher} failed with error: {e}. Continuing with other fetchers."
    #                 )
    #     logger.debug("Start Wlan Details Fetcher loop")
    #     for wlan in cast(list[str], res["wlan_data"]):
    #         try:
    #             logger.info(f"Running WLAN details fetcher for WLAN: {wlan}")
    #             points = wlan_trhougput_trends(aruba_api, wlan)
    #             res_points.extend(points)
    #             logger.debug("WLAN details fetcher for %s returned %s points: %s", wlan, len(points), points)
    #         except Exception as e:
    #             logger.warning(f"WLAN details fetcher for {wlan} failed with error: {e}. Continuing with other WLANs.")
    #     logger.debug("Start Device Hw Details Fetcher loop")
    #     for device in cast(list[dict[str, Any]], res["device_data"]):
    #         serial_number = cast(str, device.get("serial_number", "unknown"))
    #         try:
    #             device_type = cast(str, device.get("device_type", "unknown"))
    #             if device_type not in device_details_func:
    #                 logger.warning(
    #                     f"Device type {device_type} for device with serial number {serial_number} is not supported. "
    #                     "Skipping device details fetchers for this device."
    #                 )
    #                 continue
    #             for device_details, func in device_details_func.get(device_type, {}).items():
    #                 logger.info(
    #                     f"Running device details fetcher {device_details}"
    #                     f"for device with serial number: {serial_number}"
    #                 )
    #                 points = func(aruba_api, serial_number)
    #                 res_points.extend(points)
    #                 logger.debug(
    #                     "Device details fetcher %s for device %s returned %s points: %s",
    #                     device_details,
    #                     serial_number,
    #                     len(points),
    #                     points,
    #                 )
    #         except Exception as e:
    #             logger.warning(
    #                 f"Device details fetcher for {serial_number} failed with error: {e}."
    #                 "Continuing with other devices."
    #             )
    
    #TODO: Add fortigate fetcher private function
    res_points.extend(_run_fortigate_fetcher(credentials=creds, env_vars=env_vars, logger=logger))

    logger.info("Finished all fetchers. Storing points in InfluxDB...")
    _ = asyncio.run(store_points(points=res_points, influx_conf=env_vars.influxdb, debug_mode=env_vars.debug_mode))

    logger.debug(f"Finished all fetchers. Total points collected: {len(res_points)}")
    return len(res_points)


def run_forever(env_vars: EnvironmentsVariables, shutdown_event: threading.Event) -> None:
    """Run AFIRA collection cycles indefinitely until a shutdown signal is received."""
    logger = logging.getLogger("AFIRA.Main")
    iteration: int = 0

    logger.info("AFIRA loop started with %s seconds between iterations.", env_vars.loop_sleep_seconds)

    while not shutdown_event.is_set():
        start_time = datetime.now()
        iteration += 1
        try:
            logger.info("Starting AFIRA iteration")
            point_count = run_once(env_vars=env_vars)
            logger.info(
                "AFIRA iteration completed at %s with %s points.",
                precisedelta(datetime.now() - start_time),
                point_count,
            )
        except Exception:
            logger.exception(
                "AFIRA iteration %s failed. Sleeping %s seconds before retrying. After duration: %s",
                iteration,
                env_vars.loop_sleep_seconds,
                precisedelta(datetime.now() - start_time),
            )

        if shutdown_event.is_set():
            break

        logger.info("AFIRA sleeping %s seconds before the next iteration.", env_vars.loop_sleep_seconds)
        shutdown_event.wait(timeout=int(env_vars.loop_sleep_seconds))

    logger.info("AFIRA loop stopped.")


def _build_shutdown_signal_handler(shutdown_event: threading.Event, logger: logging.Logger) -> ShutdownSignalHandler:
    """Create a signal handler that asks the AFIRA loop to stop gracefully."""

    def _handle_shutdown_signal(signum: int, _frame: FrameType | None) -> None:
        try:
            signal_name = signal.Signals(signum).name
        except ValueError:
            signal_name = str(signum)

        logger.info("AFIRA received shutdown signal %s. Exiting after the current iteration.", signal_name)
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
        try:
            run_forever(env_vars=env_vars, shutdown_event=shutdown_event)
        except Exception as e:
            log.critical(msg=f"AFIRA encountered a critical error: {e}", exc_info=True, stack_info=True)
            raise SystemExit(1) from e
        finally:
            log.info("AFIRA shutdown complete.")

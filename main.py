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
    gateways_hw_data,
    switch_data,
    switch_hw_data,
)
from lib.sites_details import clients_data, device_locations, web_app_data, wifi_clients_loc, wlan_trhougput_trends
from models import EnvironmentsVariables, HPEOAuth2Client

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
    }
    device_details_func: dict[str, dict[str, Callable[[HPEOAuth2Client, str], list[Point]]]] = {
        "ACCESS_POINT": {
            "ap_data": ap_data,
            "ap_cpu_util": ap_cpu_util,
            "ap_mem_util": ap_mem_util,
            "ap_power_util": ap_power_util,
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

    with HPEOAuth2Client(**creds["new_central"]) as aruba_api:
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
                    logger.debug("Site details fetcher %s returned %s points: %s", fetcher, len(points), points)
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
                logger.debug("WLAN details fetcher for %s returned %s points: %s", wlan, len(points), points)
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
                        f"Running device details fetcher {device_details}"
                        f"for device with serial number: {serial_number}"
                    )
                    points = func(aruba_api, serial_number)
                    res_points.extend(points)
                    logger.debug(
                        "Device details fetcher %s for device %s returned %s points: %s",
                        device_details,
                        serial_number,
                        len(points),
                        points,
                    )
            except Exception as e:
                logger.warning(
                    f"Device details fetcher for {serial_number} failed with error: {e}."
                    "Continuing with other devices."
                )
        # TODO: Add Radio Data Fetcher loop here when implemented

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

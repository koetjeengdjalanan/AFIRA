"""AFIRA Main Module."""

import asyncio
import logging
import queue
from typing import Any, Callable

from influxdb_client.client.write.point import Point
from rich.console import Console

from config.context import logging_context
from config.db_check import test_db_setup
from helper import logging as logging_helper
from helper.db import retrieve_creds, store_points
from lib.data_fetcher import device_data, site_health, wlan_data
from lib.device_details import ap_cpu_util, ap_data, ap_mem_util, ap_power_util, switch_data, switch_hw_data
from lib.sites_details import clients_data, device_locations, web_app_data, wifi_clients_loc, wlan_trhougput_trends
from models import EnvironmentsVariables, HPEOAuth2Client

FetcherItems = list[str] | list[dict[str, Any]]
FetcherResult = tuple[FetcherItems, list[Point]]
FetcherReturn = tuple[FetcherItems | None, list[Point] | None] | None
FetcherFunction = Callable[[HPEOAuth2Client], FetcherReturn]


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


def main() -> None:
    """AFIRA Entry point."""
    logger = logging.getLogger("AFIRA.Main")
    creds: dict[str, Any] = retrieve_creds()
    res_points: list[Point] = []
    res: dict[str, Any] = {}
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
            for site_id in res["site_health"]:
                try:
                    logger.info(f"Running site details fetcher: {fetcher}")
                    points = func(aruba_api, site_id)
                    res_points.extend(points)
                    logger.debug(f"Site details fetcher {fetcher} returned {len(points)} points", points)
                except Exception as e:
                    logger.warning(
                        f"Site details fetcher {fetcher} failed with error: {e}. Continuing with other fetchers."
                    )
        logger.debug("Start Wlan Details Fetcher loop")
        for wlan in res["wlan_data"]:
            try:
                logger.info(f"Running WLAN details fetcher for WLAN: {wlan}")
                points = wlan_trhougput_trends(aruba_api, wlan)
                res_points.extend(points)
                logger.debug(f"WLAN details fetcher for {wlan} returned {len(points)} points", points)
            except Exception as e:
                logger.warning(f"WLAN details fetcher for {wlan} failed with error: {e}. Continuing with other WLANs.")
        logger.debug("Start Device Hw Details Fetcher loop")
        for device in res["device_data"]:
            try:
                match device.get("device_type", ""):
                    case "ACCESS_POINT":
                        res_points.extend(ap_data(aruba_api, device.get("serial_number")))
                        res_points.extend(ap_cpu_util(aruba_api, device.get("serial_number")))
                        res_points.extend(ap_mem_util(aruba_api, device.get("serial_number")))
                        res_points.extend(ap_power_util(aruba_api, device.get("serial_number")))
                    case "SWITCH":
                        res_points.extend(switch_data(aruba_api, device.get("serial_number")))
                        res_points.extend(switch_hw_data(aruba_api, device.get("serial_number")))
                logger.debug(
                    f"Device details fetcher for {device.get('serial_number', 'unknown')} returned points",
                    res_points[-1],
                )
            except Exception as e:
                logger.warning(
                    f"Device details fetcher for {device.get('serial_number', 'unknown')} failed with error: {e}."
                    "Continuing with other devices."
                )
        # TODO: Add Radio Data Fetcher loop here when implemented

    _ = asyncio.run(store_points(points=res_points, influx_conf=env_vars.influxdb, debug_mode=env_vars.debug_mode))

    logger.debug(f"Finished all fetchers. Total points collected: {len(res_points)}")


if __name__ == "__main__":
    console: Console = Console()
    console.print("[bold green]Starting AFIRA...[/bold green]")

    global env_vars
    env_vars = EnvironmentsVariables()

    log_q: queue.Queue = queue.Queue(maxsize=-1)

    with logging_context(
        settings=env_vars.logging, console=console, log_queue=log_q, level=env_vars.log_level
    ) as log_listener:
        logging_helper.worker_logger(log_queue=log_q, **env_vars.logging.model_dump())
        log = logging.getLogger("AFIRA")
        log.info("AFIRA is starting")
        try:
            test_db_setup(setting=env_vars.influxdb)
            main()
        except KeyboardInterrupt:
            log.info("AFIRA received shutdown signal (KeyboardInterrupt). Exiting gracefully...")
        except ConnectionError as ce:
            log.error(f"AFIRA encountered a connection error: {ce}", exc_info=False, stack_info=False)
        except Exception as e:
            log.critical(msg=f"AFIRA encountered a critical error: {e}", exc_info=True, stack_info=True)
            raise SystemExit(1) from e
        finally:
            log.info("AFIRA finished iterations, shutting down!")

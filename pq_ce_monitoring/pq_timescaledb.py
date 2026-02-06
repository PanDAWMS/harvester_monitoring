import time
import traceback
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from .pq_es import PandaQEs

from baseclasses.cricinfo import CricInfo
from accounting.error_accounting import Errors
from logger import ServiceLogger

from baseclasses.tsdbbaseclass import TimescaleDbBaseClass

_logger = ServiceLogger("pq_timescaledb", __file__).logger


class TimescalePQ(TimescaleDbBaseClass):
    def __init__(self, settings_path: str):
        self.cric = CricInfo(settings_path)
        self.url_pq = "https://atlas-cric.cern.ch/api/atlas/pandaqueue/query/?json"
        super().__init__(settings_path)

    def _write_batches_parallel(self, batches, max_workers: int = 1):
        def _one(name, pts):
            if not pts:
                return (name, "skip", 0)
            try:
                self.connection.write_points(pts)
                return (name, "ok", len(pts))
            except Exception as ex:
                _logger.error(f"[Timescale write] batch={name} points={len(pts)} err={ex}")
                _logger.debug(traceback.format_exc())
                return (name, "err", len(pts))

        if max_workers <= 1:
            for name, pts in batches:
                _one(name, pts)
            return

        with ThreadPoolExecutor(max_workers=1) as pool:
            futures = [pool.submit(_one, name, pts) for name, pts in batches]
            for fut in as_completed(futures):
                name, status, n = fut.result()
                if status == "ok":
                    _logger.debug(f"[Timescale write] batch={name} OK points={n}")

    def write_data(self, tdelta: int = 60):
        date_key = datetime.now()
        es = PandaQEs(self.path)

        harvester_queues, harvester_computingelements, errors_df = es.get_workers_info(
            tdelta=tdelta, time="endtime"
        )

        errors_object = Errors("patterns.txt")

        harvester_queues_errors = {}
        harvester_computingelements_errors = {}

        for queuename in harvester_queues:
            if "errors" in harvester_queues[queuename]:
                harvester_queues_errors = errors_object.errors_accounting(
                    queuename,
                    harvester_queues[queuename]["errors"],
                    harvester_queues_errors,
                    harvester_queues[queuename]["badworkers"],
                )
                harvester_computingelements_errors[queuename] = {}

            for ce in harvester_computingelements[queuename]:
                if "errors" in harvester_computingelements[queuename][ce]:
                    harvester_computingelements_errors[queuename] = errors_object.errors_accounting(
                        ce,
                        harvester_computingelements[queuename][ce]["errors"],
                        harvester_computingelements_errors[queuename],
                        harvester_computingelements[queuename][ce]["badworkers"],
                    )

        q_keys = list(harvester_queues.keys())
        q_errors_keys = list(harvester_queues_errors.keys())

        harvester_queues_json = []
        harvester_queues_errors_json = []

        harvester_ce_json = []
        harvester_ce_errors_json = []

        dt_min = date_key.replace(second=0, microsecond=0)
        time_stamp = int(time.mktime(dt_min.timetuple()))

        queues = self.cric.get_cric_info(self.url_pq)

        # --- PQ level ---
        for queuename, queue in queues.items():
            if queuename in q_keys:
                harvester_queues_json.append(
                    {
                        "measurement": "computingsites",
                        "tags": {
                            "computingsite": queuename,
                            "cloud": queue["cloud"],
                            "atlas_site": queue["atlas_site"],
                            "status": queue["status"],
                            "harvester": queue["harvester"],
                            "resource_type": queue["resource_type"],
                        },
                        "time": time_stamp,
                        "fields": {
                            "totalworkers": harvester_queues[queuename]["totalworkers"],
                            "goodworkers": harvester_queues[queuename]["goodworkers"],
                            "badworkers": harvester_queues[queuename]["badworkers"],
                        },
                    }
                )

                if queuename in q_errors_keys:
                    for error in harvester_queues_errors[queuename]:
                        harvester_queues_errors_json.append(
                            {
                                "measurement": "computingsite_errors",
                                "tags": {
                                    "computingsite": queuename,
                                    "cloud": queue["cloud"],
                                    "atlas_site": queue["atlas_site"],
                                    "errordesc": error,
                                    "harvester": queue["harvester"],
                                    "status": queue["status"],
                                    "resource_type": queue["resource_type"],
                                },
                                "time": time_stamp,
                                "fields": {
                                    "totalworkers": harvester_queues[queuename]["totalworkers"],
                                    "goodworkers": harvester_queues[queuename]["goodworkers"],
                                    "badworkers": harvester_queues[queuename]["badworkers"],
                                    "error_count": harvester_queues_errors[queuename][error]["error_count"],
                                    "total_error_count": harvester_queues_errors[queuename][error]["total_error_count"],
                                },
                            }
                        )
            else:
                harvester_queues_json.append(
                    {
                        "measurement": "computingsites",
                        "tags": {
                            "computingsite": queuename,
                            "cloud": queue["cloud"],
                            "atlas_site": queue["atlas_site"],
                            "status": queue["status"],
                            "harvester": queue["harvester"],
                            "resource_type": queue["resource_type"],
                        },
                        "time": time_stamp,
                        "fields": {"totalworkers": 0, "goodworkers": 0, "badworkers": 0},
                    }
                )

        # --- CE level ---
        for queuename in harvester_computingelements:
            if queuename not in queues:
                status = cloud = atlas_site = harvester = resource_type = "none"
            else:
                status = queues[queuename]["status"]
                cloud = queues[queuename]["cloud"]
                atlas_site = queues[queuename]["atlas_site"]
                harvester = queues[queuename]["harvester"]
                resource_type = queues[queuename]["resource_type"]

            for ce in harvester_computingelements[queuename]:
                harvester_ce_json.append(
                    {
                        "measurement": "computingelements",
                        "tags": {
                            "computingsite": queuename,
                            "computingelement": ce,
                            "status": status,
                            "cloud": cloud,
                            "atlas_site": atlas_site,
                            "harvester": harvester,
                            "resource_type": resource_type,
                        },
                        "time": time_stamp,
                        "fields": {
                            "totalworkers": harvester_computingelements[queuename][ce]["totalworkers"],
                            "goodworkers": harvester_computingelements[queuename][ce]["goodworkers"],
                            "badworkers": harvester_computingelements[queuename][ce]["badworkers"],
                        },
                    }
                )

                if (
                    queuename in harvester_computingelements_errors
                    and ce in harvester_computingelements_errors[queuename]
                ):
                    for error in harvester_computingelements_errors[queuename][ce]:
                        harvester_ce_errors_json.append(
                            {
                                "measurement": "computingelement_errors",
                                "tags": {
                                    "computingsite": queuename,
                                    "computingelement": ce,
                                    "errordesc": error,
                                    "status": status,
                                    "cloud": cloud,
                                    "atlas_site": atlas_site,
                                    "harvester": harvester,
                                    "resource_type": resource_type,
                                },
                                "time": time_stamp,
                                "fields": {
                                    "totalworkers": harvester_computingelements[queuename][ce]["totalworkers"],
                                    "goodworkers": harvester_computingelements[queuename][ce]["goodworkers"],
                                    "badworkers": harvester_computingelements[queuename][ce]["badworkers"],
                                    "error_count": harvester_computingelements_errors[queuename][ce][error]["error_count"],
                                    "total_error_count": harvester_computingelements_errors[queuename][ce][error][
                                        "total_error_count"
                                    ],
                                },
                            }
                        )

        _logger.debug("Prepared points, starting write...")

        self._write_batches_parallel(
            [
                ("computingsites", harvester_queues_json),
                ("computingsite_errors", harvester_queues_errors_json),
                ("computingelements", harvester_ce_json),
                ("computingelement_errors", harvester_ce_errors_json),
            ],
            max_workers=4,
        )

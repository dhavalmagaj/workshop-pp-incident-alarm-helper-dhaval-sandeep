import logging
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Tuple, Union

from .snowflake_db import SnowFlakeDb


_GET_ALARM_INCIDENT_QUERY = """
SELECT
    TBL_ALARM."INCIDENT_ID",  
    TBL_ALARM."INCIDENT_TIME",
    TBL_ALARM."ALARM_ID",
    TBL_ALARM."DEVICE_ID",
    (CASE WHEN TBL_ALERT_FEEDBACK."CAUSE" = 1 THEN TBL_ALERT_FEEDBACK."CAUSE_OTHER" ELSE TBL_FEEDBACK_CAUSE."NAME" END) AS "FEEDBACK"
FROM "DW"."MASTER"."ALARMS" AS TBL_ALARM
    LEFT JOIN "DL"."DYNAMODB"."PROD_ALERT_FEEDBACK" AS TBL_ALERT_FEEDBACK ON TBL_ALARM."INCIDENT_ID" = TBL_ALERT_FEEDBACK."INCIDENT_ID"
    LEFT JOIN "DL"."IMPORT"."FEEDBACK_CAUSE" AS TBL_FEEDBACK_CAUSE ON TBL_ALERT_FEEDBACK."CAUSE" = TBL_FEEDBACK_CAUSE."ID"
WHERE TBL_ALARM."ALARM_ID" IN (:1)
    AND TO_TIMESTAMP_NTZ(TBL_ALARM."INCIDENT_TIME") >= :2
    AND TO_TIMESTAMP_NTZ(TBL_ALARM."INCIDENT_TIME") <= :3;
"""


def get_alarms(
    start_date: datetime, end_date: datetime, alarm_ids: List[int]
) -> List[Dict]:
    """Get alarm incident"""

    if not start_date:
        raise ValueError("Required 'start_date'!")

    if not end_date:
        raise ValueError("Required 'end_date'!")

    if not alarm_ids:
        raise ValueError("Required 'alarm_ids'!")

    logging.info(
        "Start fetching alarms({}) incident for time: {} to {}".format(
            alarm_ids, start_date, end_date
        )
    )
    sf_db = SnowFlakeDb()

    # Fetching alarm from snowflake
    alarm_incidents = sf_db.fetchall(
        _GET_ALARM_INCIDENT_QUERY,
        params=(",".join(alarm_ids), start_date, end_date),
        dict_cursor=True,
    )
    logging.info(
        "End fetching alarm({}) incident for time: {} to {}".format(
            alarm_ids, start_date, end_date
        )
    )
    logging.info(alarm_incidents)

    return alarm_incidents

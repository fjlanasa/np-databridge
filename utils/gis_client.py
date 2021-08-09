import json
import os
from typing import Dict
import urllib
import requests
from requests.models import Response

from utils.constants import (
    GISActiveLitigationsFields,
    GISLitigationHistoryFields,
    GIS_HOST,
    GIS_FEATURE_SERVER_PATH,
    GIS_ACTIVE_LITIGATION_TABLE_ID,
    GIS_LITIGATION_HISTORY_TABLE_ID,
    GIS_LITIGATION_HISTORY_TABLE_ID,
)
from utils.logging import logger


def handle_api_response(res: Response):
    if res.status_code != 200:
        logger.info(f"Non 200 status code for gis request {res}")
    else:
        content = res.json()
        if content.get("features") is not None:
            return content
        else:
            logger.info("Unexpected response body for gis request {content}")
    raise Exception


class GISClient:
    def __init__(
        self,
        host=GIS_HOST,
        feature_server_path=GIS_FEATURE_SERVER_PATH,
        active_litigation_table_id=GIS_ACTIVE_LITIGATION_TABLE_ID,
        litigation_history_table_id=GIS_LITIGATION_HISTORY_TABLE_ID,
    ):
        self.host = host
        self.feature_server_path = feature_server_path
        self.active_litigation_table_id = active_litigation_table_id
        self.litigation_history_table_id = litigation_history_table_id

    def build_query_url(self, table_id, query_params: Dict = {}, query_path = "query"):
        default_query_params = {
            "f": "pjson",
            "returnGeometry": "true",
        }
        params = {**query_params, **default_query_params}
        query_string = urllib.parse.urlencode(params)
        url = (
            os.path.join(self.host, self.feature_server_path, str(table_id), query_path)
            + "?"
            + query_string
        )
        return url

    def get_active_litigations(
        self,
        query_start_datetime=None,
        fields=[
            GISActiveLitigationsFields.OBJECT_ID.value,
            ## sr number
            GISActiveLitigationsFields.INCIDENT_NUMBER.value,
            GISActiveLitigationsFields.PARCEL_ID.value,
            GISActiveLitigationsFields.CITY_FILE_NO.value,
            ## subdistrict
            GISActiveLitigationsFields.SUB_DISTRICT.value,
            GISActiveLitigationsFields.NPA_INSPECT_SUMMARY.value,
            GISActiveLitigationsFields.CIVIL_WARRANT.value,
            ## location
            GISActiveLitigationsFields.LOCATION.value,
            GISActiveLitigationsFields.NEXT_COURT_DATE.value,
            ## property owner
            GISActiveLitigationsFields.PROPERTY_OWNER.value,
            ## defendent
            GISActiveLitigationsFields.DEFENDENT.value,
            GISActiveLitigationsFields.COURT_STATUS.value,
            GISActiveLitigationsFields.LATEST_COURT_NOTES.value,
            GISActiveLitigationsFields.CREATION_DATE.value,
            GISActiveLitigationsFields.LAST_MODIFIED_DATE.value,
        ],
    ):
        """ """
        query_params = {
            "where": f"last_modified_date > '{query_start_datetime}'"
            if query_start_datetime
            else "",
            "outFields": ",".join(fields),
        }
        url = self.build_query_url(self.active_litigation_table_id, query_params)
        res = requests.get(url)
        return handle_api_response(res)

    def get_dismissed_statuses(
        self,
        fields=[
            GISLitigationHistoryFields.CIVIL_WARRANT.value,
            GISLitigationHistoryFields.DISMISS_STATUS.value,
            GISLitigationHistoryFields.DISMISSED_CONDITION.value,
            GISLitigationHistoryFields.NEXT_COURT_DATE.value,
        ],
    ):
        query_params = {
            "where": "DismissStatus IS NOT NULL OR DismissedCondition IS NOT NULL",
            "outFields": ",".join(fields),
        }
        url = self.build_query_url(self.litigation_history_table_id, query_params)
        res = requests.get(url)
        return handle_api_response(res)

    def update_litigation():
        pass

    def get_attachments(self, object_id):
        url = os.path.join(
            self.host,
            self.feature_server_path,
            self.active_litigation_table_id,
            str(object_id),
            "attachments",
        )
        return requests.get(url, params={"f": "pjson"}).json()["attachmentInfos"]

    def get_attachment(self, feature_object_id, attachment_object_id):
        url = os.path.join(
            self.host,
            self.feature_server_path,
            self.active_litigation_table_id,
            str(feature_object_id),
            "attachments",
            str(attachment_object_id),
        )
        return requests.get(url).content

    def add_litigation_history(self, features):
        url = os.path.join(
            self.host,
            self.feature_server_path,
            self.litigation_history_table_id,
            "addFeatures"
        )
        return requests.post(url, params={"f": "json", "features": json.dumps(features)})

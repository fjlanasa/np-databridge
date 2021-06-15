import json
import os
from typing import Dict
import urllib
import requests
from requests.models import Response

from utils.constants import (
    GISFields,
    GIS_HOST,
    GIS_FEATURE_SERVER_PATH,
    GIS_ACTIVE_LITIGATION_TABLE_ID,
    GIS_LITIGATION_HISTORY_TABLE_ID,
    GIS_LITIGATION_HISTORY_TABLE_ID,
)
from utils.logging import logger


def handle_api_response(res: Response):
    if res.status_code != 200:
        logger.info("Non 200 status code for gis request {res}")
    else:
        content = res.json()
        if content.get("features") is not None:
            return content["features"]
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

    def build_query_url(self, table_id, query_params: Dict = {}):
        default_query_params = {
            "f": "pjson",
            "returnGeometry": "true",
        }
        params = {**query_params, **default_query_params}
        query_string = urllib.parse.urlencode(params)
        url = (
            os.path.join(self.host, self.feature_server_path, str(table_id), "query")
            + "?"
            + query_string
        )
        return url

    def build_where_clause(self, field, query_start_datetime=None):
        return (
            f"&where={field} > {query_start_datetime}" if query_start_datetime else ""
        )

    def get_active_litigations(
        self,
        query_start_datetime=None,
        fields=[
            GISFields.OBJECT_ID.value,
            ## sr number
            GISFields.INCIDENT_NUMBER.value,
            GISFields.PARCEL_ID.value,
            GISFields.CITY_FILE_NO.value,
            ## subdistrict
            GISFields.SUB_DISTRICT.value,
            GISFields.NPA_INSPECT_SUMMARY.value,
            GISFields.CIVIL_WARRANT.value,
            ## location
            GISFields.LOCATION.value,
            GISFields.NEXT_COURT_DATE.value,
            ## property owner
            GISFields.PROPERTY_OWNER.value,
            ## defendent
            GISFields.DEFENDENT.value,
            GISFields.COURT_STATUS.value,
            GISFields.LATEST_COURT_NOTES.value,
            GISFields.CREATION_DATE.value,
            GISFields.LAST_MODIFIED_DATE.value,
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

    def get_litigation_history(
        self,
        query_start_datetime=None,
        fields=[
            GISFields.OBJECT_ID.value,
            GISFields.INCIDENT_NUMBER.value,
            GISFields.CIVIL_WARRANT.value,
            GISFields.COURT_NOTES.value,
            GISFields.NEXT_COURT_DATE.value,
        ],
    ):
        query_params = {
            "where": f"last_edited_date > '{query_start_datetime}'"
            if query_start_datetime
            else "",
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

    def update_features(self, features):
        url = os.path.join(
            self.host,
            self.feature_server_path,
            self.active_litigation_table_id,
            "updateFeatures"
        )
        return requests.post(url, params={"f": "json", "features": json.dumps(features)})

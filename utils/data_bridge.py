import os
import datetime
import json
from dataclasses import dataclass, asdict
from collections import defaultdict
from typing import Dict, List, Optional, TypedDict

import requests
from utils.constants import (
    BASE_DATA_DIR,
    ClioCustomFieldNames,
    GISActiveLitigationsFields,
    GISLitigationHistoryFields,
)
from utils.gis_client import GISClient
from utils.clio_client import ClioApiClient, take_one
from utils.logging import logger

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

s = requests.Session()
retries = Retry(total=10, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
s.mount("http://", HTTPAdapter(max_retries=retries))
s.mount("https://", HTTPAdapter(max_retries=retries))

DATE_FORMAT = "%Y-%m-%dT%H:%M:%S+00:00"


@dataclass
class GISIncident:
    object_id: int
    created: int
    updated: int
    incident_number: str
    parcel_id: str
    city_file_no: str
    sub_district: str
    npa_inspect_summary: str
    court_status: str
    location: str
    next_court_date: int
    property_owner: str
    defendent: str
    civil_warrant: str
    latest_court_notes: str
    geometry: Dict[str, float]
    dismiss_status: str = None
    dismissed_condition: str = None


@dataclass
class GISLitigationHistory:
    object_id: int
    court_notes: str
    next_court_date: int
    civil_warrant: str
    incident_number: str


@dataclass
class GISAttachment:
    id: int
    litigation_object_id: str
    civil_warrant: str
    content_type: str
    size: int
    name: str


class ClioCalendarEntry:
    id: str
    matter: dict
    name: str

    def __init__(self, id, matter, detail):
        self.id = id
        self.matter = ClioMatter(matter) if matter else None
        self.detail = detail

    def to_gis_request_feature(self):
        if self.matter:
            return {
                "attributes": {
                    GISActiveLitigationsFields.OBJECT_ID.value: self.matter.object_id,
                    GISActiveLitigationsFields.LATEST_COURT_NOTES.value: self.detail,
                },
                "geometry": {"x": self.matter.geo_x, "y": self.matter.geo_y},
            }
        else:
            return None

    def __dict__(self):
        return {
            "id": self.id,
            "matter": self.matter.input_doc if self.matter else None,
            "detail": self.detail,
        }


@dataclass
class ClioClient:
    id: str
    name: str


@dataclass
class ClioGroup:
    id: str
    name: str


@dataclass
class ClioPracticeArea:
    id: str
    name: str


@dataclass
class ClioCalendar:
    id: str
    name: str = None


class ClioCustomField:
    def __init__(
        self, id=None, etag=None, name=None, field_type=None, picklist_options=[]
    ):
        self.id = id
        self.etag = etag
        self.name = name
        self.picklist_options = picklist_options

    def get_option_id_by_name(self, name):
        options = [
            option for option in self.picklist_options if option["option"] == name
        ]
        if bool(options):
            return options[0]["id"]
        else:
            return None


class ClioCustomFields:
    def __init__(self, fields_asset: ClioCustomField):
        self.field_ids = [field["id"] for field in fields_asset]
        self.fields_by_id = {
            field["id"]: ClioCustomField(**field) for field in fields_asset
        }
        self.fields_by_name = {
            field["name"]: ClioCustomField(**field) for field in fields_asset
        }

    def get_field_id_by_name(self, field_name):
        field = self.fields_by_name.get(field_name)
        return field.id if field else None

    def get_field_name_by_id(self, field_id):
        field = self.fields_by_id.get(field_id)
        return field.name if field else None


class ClioPickListOptions:
    def __init__(self, data):
        self.option_ids_by_name = {option["name"]: option["id"] for option in data}


class ClioMatter:
    def __init__(self, matter, next_court_date=None, court_notes=None):
        self.id = matter["id"]
        self.input_doc = matter
        object_id = [
            value
            for value in matter["custom_field_values"]
            if value["field_name"] == ClioCustomFieldNames.GIS_OBJECT_ID.value
        ]
        civil_warrant = [
            value
            for value in matter["custom_field_values"]
            if value["field_name"] == ClioCustomFieldNames.CIVIL_WARRANT.value
        ]
        self.civil_warrant = civil_warrant[0]["value"] if civil_warrant else None
        parcel_id = [
            value
            for value in matter["custom_field_values"]
            if value["field_name"] == ClioCustomFieldNames.PARCEL_ID.value
        ]
        self.parcel_id = parcel_id[0]["value"] if parcel_id else None
        incident_number = [
            value
            for value in matter["custom_field_values"]
            if value["field_name"] == ClioCustomFieldNames.INCIDENT_NUMBER.value
        ]
        self.incident_number = incident_number[0]["value"] if incident_number else None
        address = [
            value
            for value in matter["custom_field_values"]
            if value["field_name"] == ClioCustomFieldNames.LOCATION.value
        ]
        self.address = address[0]["value"] if address else None
        sub_district = [
            value
            for value in matter["custom_field_values"]
            if value["field_name"] == ClioCustomFieldNames.SUB_DISTRICT.value
        ]
        self.sub_district = sub_district[0]["value"] if sub_district else None
        self.object_id = object_id[0]["value"] if object_id else None
        court_status = [
            value
            for value in matter["custom_field_values"]
            if value["field_name"] == ClioCustomFieldNames.COURT_STATUS.value
        ]
        self.court_status = court_status[0]["value"] if court_status else None
        dismiss_status = [
            value
            for value in matter["custom_field_values"]
            if value["field_name"] == ClioCustomFieldNames.DISMISS_STATUS.value
        ]
        self.dismiss_status = dismiss_status[0]["value"] if dismiss_status else None
        dismissed_condition = [
            value
            for value in matter["custom_field_values"]
            if value["field_name"] == ClioCustomFieldNames.DISMISSED_CONDITION.value
        ]
        self.dismissed_condition = (
            dismissed_condition[0]["value"] if dismissed_condition else None
        )
        geo_x = [
            value
            for value in matter["custom_field_values"]
            if value["field_name"] == ClioCustomFieldNames.LONGITUDE.value
        ]
        self.geo_x = geo_x[0]["value"] if geo_x else None
        geo_y = [
            value
            for value in matter["custom_field_values"]
            if value["field_name"] == ClioCustomFieldNames.LATITUDE.value
        ]
        self.geo_y = geo_y[0]["value"] if geo_y else None
        self.next_court_date = next_court_date
        self.court_notes = court_notes

    def is_valid(self):
        return bool(self.object_id) and bool(self.geo_x) and bool(self.geo_y)

    def to_gis_request_feature(self):
        return {
            "attributes": {
                GISLitigationHistoryFields.OBJECT_ID.value: self.object_id,
                GISLitigationHistoryFields.CIVIL_WARRANT.value: self.civil_warrant,
                GISLitigationHistoryFields.SUB_DISTRICT.value: int(
                    self.sub_district.split("-")[0]
                ),
                GISLitigationHistoryFields.ADDRESS.value: self.address,
                GISLitigationHistoryFields.PARCEL_ID.value: self.parcel_id,
                GISLitigationHistoryFields.INCIDENT_NUMBER.value: self.incident_number,
                GISLitigationHistoryFields.COURT_STATUS.value: self.court_status,
                GISLitigationHistoryFields.NEXT_COURT_DATE.value: int(
                    datetime.datetime.fromisoformat(self.next_court_date).timestamp()
                    * 1e3
                )
                if self.next_court_date
                else None,
                GISLitigationHistoryFields.DISMISS_STATUS.value: self.dismiss_status,
                GISLitigationHistoryFields.DISMISSED_CONDITION.value: self.dismissed_condition,
            },
            "geometry": {"x": self.geo_x, "y": self.geo_y},
        }


class DataBridge:
    def __init__(
        self,
        clio_client: ClioApiClient = ClioApiClient(),
        gis_client: GISClient = GISClient(),
        data_directory_name="data",
        gis_directory_name="gis",
        clio_directory_name="clio",
        log_directory_name="log",
        queued_directory_name="queued",
        client_file_name="client.json",
        custom_fields_file_name="custom_fields.json",
        practice_area_file_name="practice_area.json",
        calendar_file_name="calendar.json",
        group_file_name="group.json",
        base_data_dir=BASE_DATA_DIR,
    ):
        ## API client instances
        self.clio_api_client = clio_client
        self.gis_client = gis_client

        ## Set base data directory path
        data_directory_path = os.path.join(
            base_data_dir,
            data_directory_name,
        )

        ## GIS directory setup
        gis_directory_path = os.path.join(data_directory_path, gis_directory_name)
        os.makedirs(gis_directory_path, exist_ok=True)
        ## File contains log of last pull
        self.gis_update_log_path = os.path.join(gis_directory_path, log_directory_name)
        try:
            with open(self.gis_update_log_path) as f:
                self.last_gis_pull = f.read()
        except:
            self.last_gis_pull = None
        self.gis_update_queue_path = os.path.join(
            gis_directory_path, queued_directory_name
        )
        ## Directory contains GIS updates to process
        self.gis_active_litigation_update_path = os.path.join(
            self.gis_update_queue_path, "active_litigations"
        )
        os.makedirs(self.gis_active_litigation_update_path, exist_ok=True)
        self.gis_ligation_attachments_update_path = os.path.join(
            self.gis_update_queue_path, "attachments"
        )
        os.makedirs(self.gis_ligation_attachments_update_path, exist_ok=True)

        ## Clio directory setup
        clio_directory_path = os.path.join(data_directory_path, clio_directory_name)
        os.makedirs(clio_directory_path, exist_ok=True)
        ## File contains log of last pull
        self.clio_update_log_path = os.path.join(
            clio_directory_path, log_directory_name
        )
        try:
            with open(self.clio_update_log_path) as f:
                self.last_clio_pull = f.read()
        except:
            self.last_clio_pull = None
        ## Directory contians Clio updates to process
        self.clio_update_queue_path = os.path.join(
            clio_directory_path, queued_directory_name
        )
        self.clio_matters_update_path = os.path.join(
            self.clio_update_queue_path, "matters"
        )
        os.makedirs(self.clio_matters_update_path, exist_ok=True)
        ## Files contain json with saved Clio resources (Client, Practice Area, Group, Custom Fields)
        ## Load Client
        self.client_path = os.path.join(clio_directory_path, client_file_name)
        client_asset = self.load_entity(self.client_path)
        self.clio_client = (
            ClioClient(id=client_asset["id"], name=client_asset["name"])
            if client_asset
            else None
        )
        ## Load Custom Fields
        self.custom_fields_path = os.path.join(
            clio_directory_path, custom_fields_file_name
        )
        custom_fields_asset = self.load_entity(self.custom_fields_path)
        self.custom_fields = custom_fields_asset
        self.custom_fields: Optional[ClioCustomFields] = (
            ClioCustomFields(custom_fields_asset) if custom_fields_asset else None
        )

        ## Load Group
        self.group_path = os.path.join(clio_directory_path, group_file_name)
        group_asset = self.load_entity(self.group_path)
        self.group = (
            ClioGroup(id=group_asset["id"], name=group_asset["name"])
            if group_asset
            else None
        )

        ## Load Practice Area
        self.practice_area_path = os.path.join(
            clio_directory_path, practice_area_file_name
        )
        practice_area_asset = self.load_entity(self.practice_area_path)
        self.practice_area = (
            ClioPracticeArea(
                id=practice_area_asset["id"], name=practice_area_asset["name"]
            )
            if practice_area_asset
            else None
        )
        ## Load Calendar
        self.clio_calendar_path = os.path.join(clio_directory_path, calendar_file_name)
        calendar_asset = self.load_entity(self.clio_calendar_path)
        self.clio_calendar = (
            ClioCalendar(id=calendar_asset["id"]) if calendar_asset else None
        )

    def make_timestamp(self):
        return datetime.datetime.utcnow().strftime(DATE_FORMAT)

    def load_entity(self, path):
        try:
            with open(path) as f:
                return json.loads(f.read())
        except:
            return None

    def save_entity(self, path, input_json, field):
        try:
            with open(path, "w") as f:
                f.write(json.dumps(input_json))
                self.__setattr__(field, input_json)
        except FileNotFoundError:
            return None

    def fetch_gis_active_litigation_features(self, max_records=None):
        return [
            GISIncident(
                object_id=x["attributes"].get(
                    GISActiveLitigationsFields.OBJECT_ID.value
                ),
                updated=x["attributes"].get(
                    GISActiveLitigationsFields.LAST_MODIFIED_DATE.value
                ),
                created=x["attributes"].get(
                    GISActiveLitigationsFields.CREATION_DATE.value
                ),
                incident_number=x["attributes"].get(
                    GISActiveLitigationsFields.INCIDENT_NUMBER.value
                ),
                parcel_id=x["attributes"].get(
                    GISActiveLitigationsFields.PARCEL_ID.value
                ),
                city_file_no=x["attributes"].get(
                    GISActiveLitigationsFields.CITY_FILE_NO.value
                ),
                sub_district=x["attributes"].get(
                    GISActiveLitigationsFields.SUB_DISTRICT.value
                ),
                npa_inspect_summary=x["attributes"].get(
                    GISActiveLitigationsFields.NPA_INSPECT_SUMMARY.value
                ),
                court_status=x["attributes"].get(
                    GISActiveLitigationsFields.COURT_STATUS.value
                ),
                location=x["attributes"].get(GISActiveLitigationsFields.LOCATION.value),
                next_court_date=x["attributes"].get(
                    GISActiveLitigationsFields.NEXT_COURT_DATE.value
                ),
                property_owner=x["attributes"].get(
                    GISActiveLitigationsFields.PROPERTY_OWNER.value
                ),
                defendent=x["attributes"].get(
                    GISActiveLitigationsFields.DEFENDENT.value
                ),
                civil_warrant=x["attributes"].get(
                    GISActiveLitigationsFields.CIVIL_WARRANT.value
                ),
                latest_court_notes=x["attributes"].get(
                    GISActiveLitigationsFields.LATEST_COURT_NOTES.value
                ),
                geometry=x["geometry"],
            )
            for x in self.gis_client.get_active_litigations(
                query_start_datetime=self.last_gis_pull
            )["features"][0:max_records]
        ]

    def log_gis_active_litigation_features(
        self, timestamp, active_litigation_features: List[GISIncident]
    ):
        if bool(active_litigation_features):
            active_litigation_f = open(
                os.path.join(self.gis_active_litigation_update_path, timestamp), "w"
            )
            for incident in active_litigation_features:
                logger.info(f"Logging update to litigation {incident}")
                active_litigation_f.write(json.dumps(asdict(incident)))
                active_litigation_f.write("\n")
            active_litigation_f.close()

    def fetch_active_litigation_features_attachments(
        self, active_litigation_features: List[GISIncident]
    ) -> List[GISAttachment]:
        attachments = []
        for incident in active_litigation_features:
            object_id = incident.object_id
            attachment_infos = self.gis_client.get_attachments(object_id)
            for attachment in attachment_infos:
                obj = GISAttachment(
                    litigation_object_id=object_id,
                    civil_warrant=incident.civil_warrant,
                    id=attachment["id"],
                    content_type=attachment["contentType"],
                    size=attachment["size"],
                    name=attachment["name"],
                )
                attachments.append(obj)
        return attachments

    def log_gis_attachments(self, timestamp, attachments: List[GISAttachment]):
        attachments_file = open(
            os.path.join(self.gis_ligation_attachments_update_path, timestamp), "w"
        )
        for obj in attachments:
            logger.info(
                f"Logging attachment for civil warrant number {obj.civil_warrant}: {obj}"
            )
            attachments_file.write(json.dumps(asdict(obj)))
            attachments_file.write("\n")
        attachments_file.close()

    def gis_to_clio_migration(self, max_records=None):
        now = self.make_timestamp()
        logger.info(f"Fetching active litigations updated since {self.last_gis_pull}")
        active_litigation_features = self.fetch_gis_active_litigation_features(
            max_records
        )
        logger.info(
            f"Fetched {len(active_litigation_features)} active litigation features"
        )
        dismissed_statuses = self.gis_client.get_dismissed_statuses()["features"]
        last_dismissed_statuses_by_civil_warrant_number = {
            status["attributes"][
                GISLitigationHistoryFields.CIVIL_WARRANT.value
            ]: status["attributes"]
            for status in dismissed_statuses
        }
        for feature in active_litigation_features:
            feature.dismiss_status = (
                last_dismissed_statuses_by_civil_warrant_number.get(
                    feature.civil_warrant, {}
                ).get(GISLitigationHistoryFields.DISMISS_STATUS.value)
            )
            feature.dismissed_condition = (
                last_dismissed_statuses_by_civil_warrant_number.get(
                    feature.civil_warrant, {}
                ).get(GISLitigationHistoryFields.DISMISSED_CONDITION.value)
            )
        self.log_gis_active_litigation_features(now, active_litigation_features)
        attachments = self.fetch_active_litigation_features_attachments(
            active_litigation_features
        )
        self.log_gis_attachments(now, attachments)

        with open(self.gis_update_log_path, "w") as f:
            f.write(now)

    def pull_gis_updates(self, max_records=None):
        now = self.make_timestamp()
        logger.info(f"Fetching active litigations updated since {self.last_gis_pull}")
        active_litigation_features = self.fetch_gis_active_litigation_features(
            max_records
        )
        logger.info(
            f"Fetched {len(active_litigation_features)} active litigation features"
        )
        self.log_gis_active_litigation_features(now, active_litigation_features)
        attachments = self.fetch_active_litigation_features_attachments(
            active_litigation_features
        )
        self.log_gis_attachments(now, attachments)

        with open(self.gis_update_log_path, "w") as f:
            f.write(now)

    def create_or_update_matter(self, incident: GISIncident, migrate=False):
        matter = (
            None
            if migrate
            else self.clio_api_client.get_matter(
                self.group.id,
                civil_warrant_field_id=self.custom_fields.get_field_id_by_name(
                    ClioCustomFieldNames.CIVIL_WARRANT.value
                ),
                civil_warrant_value=incident.civil_warrant,
            )
        )
        if matter:
            logger.info(f"matter found, {incident}")
            res = self.clio_api_client.update_matter(
                matter["id"],
                {
                    "custom_field_values": self.update_custom_field_values_payload(
                        incident, matter
                    )
                },
            )
            return res
        else:
            logger.info(f"creating matter {incident}")
            res = self.clio_api_client.create_matter(
                description=incident.location,
                client_id=self.clio_client.id,
                group_id=self.group.id,
                practice_area_id=self.practice_area.id,
                custom_field_values=self.create_custom_field_values_payload(incident),
            )

            if migrate and incident.next_court_date:
                matter_id = res.json()["data"]["id"]
                date_str = self.timestamp_to_datetime_str(incident.next_court_date)
                res = self.clio_api_client.create_calendar_entry(
                    name=incident.defendent,
                    description=incident.latest_court_notes,
                    start=date_str,
                    end=date_str,
                    calendar_id=self.clio_calendar.id,
                    matter_id=matter_id,
                )
            return res

    def upload_document(self, attachment: GISAttachment, migrate=False):
        doc = None
        matter = self.clio_api_client.get_matter(
            self.group.id,
            self.custom_fields.get_field_id_by_name(
                ClioCustomFieldNames.CIVIL_WARRANT.value
            ),
            attachment.civil_warrant,
        )
        if matter:
            doc = (
                None
                if migrate
                else self.clio_api_client.get_document(
                    matter_id=matter["id"], gis_id=attachment.id
                )
            )
            if doc is None:
                url = os.path.join(
                    self.gis_client.host,
                    self.gis_client.feature_server_path,
                    str(self.gis_client.active_litigation_table_id),
                    str(attachment.litigation_object_id),
                    "attachments",
                    str(attachment.id),
                )
                res = s.get(url, stream=True)
                if res.status_code == 200:
                    res = self.clio_api_client.upload_document(
                        matter_id=matter["id"],
                        gis_id=attachment.id,
                        file_name=attachment.name,
                        file_content=res.content,
                    )
                    doc = res.json()
        return doc

    def push_gis_updates(self, migrate=False):
        litigation_update_files = sorted(
            os.listdir(self.gis_active_litigation_update_path)
        )
        for _file in litigation_update_files:
            failures = []
            file_path = os.path.join(self.gis_active_litigation_update_path, _file)
            with open(file_path, "r") as f:
                litigation_json = f.readline()
                while litigation_json:
                    litigation = GISIncident(**json.loads(litigation_json))
                    logger.info(f"uploading matter {litigation}")
                    res = None
                    try:
                        res = self.create_or_update_matter(litigation, migrate)

                        res.raise_for_status()
                    except Exception as e:

                        logger.warning(f"Failed to process matter update {litigation}")
                        logger.warning(res.content)
                        failures.append(litigation)
                    litigation_json = f.readline()
            if bool(failures):
                update_file = open(file_path, "w")
                for failure in failures:
                    update_file.write(json.dumps(asdict(failure)))
                    update_file.write("\n")
                update_file.close()
            else:
                os.remove(file_path)

        attachments_update_files = sorted(
            os.listdir(self.gis_ligation_attachments_update_path)
        )
        for _file in attachments_update_files:
            file_path = os.path.join(self.gis_ligation_attachments_update_path, _file)
            failures = []
            with open(file_path, "r") as f:
                attachment_json = f.readline()
                while attachment_json:
                    attachment = GISAttachment(**json.loads(attachment_json))
                    logger.info(f"uploading document, {attachment}")
                    try:
                        doc = self.upload_document(
                            attachment=attachment, migrate=migrate
                        )
                    except:
                        doc = None
                    if doc:
                        logger.info(
                            f"Successfully uploaded document to clio {attachment}"
                        )
                    else:
                        failures.append(attachment)
                        logger.warning(
                            f"Failed to upload document to clio {attachment}"
                        )
                    attachment_json = f.readline()
            if bool(failures):
                update_file = open(file_path, "w")
                for failure in failures:
                    update_file.write(json.dumps(asdict(failure)))
                    update_file.write("\n")
                update_file.close()
            else:
                os.remove(file_path)

    def get_all_matters(self, ids=None, updated_since=None):
        matters = []
        res = self.clio_api_client.get_matters(
            self.group.id, self.practice_area.id, ids=ids, updated_since=updated_since
        )
        if res.status_code == 200:
            body = res.json()
            matters += body["data"]
            next = body["meta"].get("paging").get("next")
            while next:
                res = self.clio_api_client.oauth.client.get(next)
                content = res.json()
                matters += content.get("data", [])
                next = res.json().get("meta", {}).get("paging", {}).get("next")
        return matters

    def pull_clio_updates(self, max_records=None):
        now = self.make_timestamp()
        logger.info(f"Pulling Clio updates at {now}")
        logger.info(f"Last Clio pull was {self.last_clio_pull}")
        logger.info("Fetching all Clio matters")
        last_clio_pull_date = self.last_clio_pull
        recently_updated_matters = self.get_all_matters(
            updated_since=last_clio_pull_date
        )[0:max_records]
        ce_res = self.clio_api_client.get_calendar_entries(
            self.clio_calendar.id, last_clio_pull_date, now
        )
        calendar_entries = sorted(
            [ce for ce in ce_res.json()["data"] if ce["matter"]],
            key=lambda x: x["start_at"],
            reverse=True,
        )
        matters_by_id = {}
        if calendar_entries:
            next_calendar_entries_by_matter_id = {
                ce["matter"]["id"]: ce for ce in calendar_entries
            }
            calendar_entry_matters = self.get_all_matters(
                ids=next_calendar_entries_by_matter_id.keys()
            )
        else:
            next_calendar_entries_by_matter_id = {}
            calendar_entry_matters = []
        matters_by_id = {
            matter["id"]: matter
            for matter in recently_updated_matters + calendar_entry_matters
        }
        logger.debug(f"Fetched {len(matters_by_id)} matters")
        if bool(matters_by_id):
            matter_f = open(os.path.join(self.clio_matters_update_path, now), "w")
            for id, matter in matters_by_id.items():
                calendar_entry = next_calendar_entries_by_matter_id.get(id, {})
                matter = ClioMatter(
                    matter,
                    calendar_entry.get("start_at"),
                    calendar_entry.get("description"),
                )
                if (
                    matter.court_status
                    == self.custom_fields.fields_by_name[
                        ClioCustomFieldNames.COURT_STATUS.value
                    ].get_option_id_by_name("Dismissed")
                    or matter.next_court_date
                ):
                    logger.info(f"Logging Clio matter update {matter.input_doc}")
                    log = {
                        "matter": matter.input_doc,
                        "next_court_date": matter.next_court_date,
                        "court_notes": matter.court_notes,
                    }
                    matter_f.write(json.dumps(log))
                    matter_f.write("\n")
            matter_f.close()
        with open(self.clio_update_log_path, "w") as f:
            f.write(now)

    def process_clio_matters(self):
        matter_update_files = sorted(os.listdir(self.clio_matters_update_path))
        for _file in matter_update_files:
            file_path = os.path.join(self.clio_matters_update_path, _file)
            matters_to_process_by_id: Dict[str, ClioMatter] = {}
            matter_id_by_gis_object_id: Dict[str, str] = {}
            failures = []
            matters_to_process = []
            with open(file_path, "r") as f:
                matter_json = f.readline()
                while matter_json:
                    details = json.loads(matter_json)
                    matter = ClioMatter(
                        matter=details["matter"],
                        next_court_date=details["next_court_date"],
                        court_notes=details["court_notes"],
                    )
                    matters_to_process.append(matter)
                    matter_json = f.readline()
            res = self.gis_client.add_litigation_history(
                [matter.to_gis_request_feature() for matter in matters_to_process]
            )
            if res.status_code == 200:
                update_results = res.json()["addResults"]
                for i, result in enumerate(update_results):
                    matter = matters_to_process[i]
                    if result["success"]:
                        logger.info(
                            f"Successfully pushed matter updates to GIS {matter}"
                        )
                    else:
                        logger.warning(f"Failed to push matter updates to GIS {matter}")
                        failures.append(
                            {
                                "matter": matter.input_doc,
                                "next_court_date": matter.next_court_date,
                                "court_notes": matter.court_notes,
                            }
                        )
            else:
                failures = [
                    {
                        "matter": matter.input_doc,
                        "next_court_date": matter.next_court_date,
                        "court_notes": matter.court_notes,
                    }
                    for matter in matters_to_process
                ]
            if failures:
                update_file = open(file_path, "w")
                for failure in failure:
                    update_file.write(json.dumps(failure))
                    update_file.write("\n")
                update_file.close()
            else:
                os.remove(file_path)

    def push_clio_updates(self):
        self.process_clio_matters()

    def timestamp_to_datetime_str(self, timestamp):
        return datetime.datetime.fromtimestamp(timestamp / 1e3).isoformat()

    def create_custom_field_values_payload(self, incident: GISIncident):
        return [
            {
                "custom_field": {
                    "id": self.custom_fields.get_field_id_by_name(
                        ClioCustomFieldNames.CIVIL_WARRANT.value
                    )
                },
                "value": incident.civil_warrant,
            },
            {
                "custom_field": {
                    "id": self.custom_fields.get_field_id_by_name(
                        ClioCustomFieldNames.INCIDENT_NUMBER.value
                    ),
                },
                "value": incident.incident_number,
            },
            {
                "custom_field": {
                    "id": self.custom_fields.get_field_id_by_name(
                        ClioCustomFieldNames.PARCEL_ID.value
                    ),
                },
                "value": incident.parcel_id,
            },
            {
                "custom_field": {
                    "id": self.custom_fields.get_field_id_by_name(
                        ClioCustomFieldNames.CITY_FILE_NO.value
                    ),
                },
                "value": incident.city_file_no,
            },
            {
                "custom_field": {
                    "id": self.custom_fields.get_field_id_by_name(
                        ClioCustomFieldNames.SUB_DISTRICT.value
                    ),
                },
                "value": incident.sub_district,
            },
            {
                "custom_field": {
                    "id": self.custom_fields.get_field_id_by_name(
                        ClioCustomFieldNames.NPA_INSPECT_SUMMARY.value
                    )
                },
                "value": incident.npa_inspect_summary,
            },
            {
                "custom_field": {
                    "id": self.custom_fields.get_field_id_by_name(
                        ClioCustomFieldNames.LOCATION.value
                    )
                },
                "value": incident.location,
            },
            {
                "custom_field": {
                    "id": self.custom_fields.get_field_id_by_name(
                        ClioCustomFieldNames.COURT_STATUS.value
                    )
                },
                "value": self.custom_fields.fields_by_name[
                    ClioCustomFieldNames.COURT_STATUS.value
                ].get_option_id_by_name(incident.court_status),
            },
            {
                "custom_field": {
                    "id": self.custom_fields.get_field_id_by_name(
                        ClioCustomFieldNames.PROPERTY_OWNER.value
                    )
                },
                "value": incident.property_owner,
            },
            {
                "custom_field": {
                    "id": self.custom_fields.get_field_id_by_name(
                        ClioCustomFieldNames.DEFENDENT.value
                    )
                },
                "value": incident.defendent,
            },
            {
                "custom_field": {
                    "id": self.custom_fields.get_field_id_by_name(
                        ClioCustomFieldNames.LONGITUDE.value
                    )
                },
                "value": str(incident.geometry["x"]),
            },
            {
                "custom_field": {
                    "id": self.custom_fields.get_field_id_by_name(
                        ClioCustomFieldNames.LATITUDE.value
                    )
                },
                "value": str(incident.geometry["y"]),
            },
            {
                "custom_field": {
                    "id": self.custom_fields.get_field_id_by_name(
                        ClioCustomFieldNames.GIS_OBJECT_ID.value
                    )
                },
                "value": incident.object_id,
            },
            {
                "custom_field": {
                    "id": self.custom_fields.get_field_id_by_name(
                        ClioCustomFieldNames.DISMISS_STATUS.value
                    ),
                    "value": incident.dismiss_status,
                }
            },
            {
                "custom_field": {
                    "id": self.custom_fields.get_field_id_by_name(
                        ClioCustomFieldNames.DISMISSED_CONDITION.value
                    ),
                    "value": incident.dismissed_condition,
                }
            },
        ]

    def update_custom_field_values_payload(self, incident: GISIncident, matter):
        custom_field_values = matter["custom_field_values"]
        npa_inspect_summary = [
            value
            for value in custom_field_values
            if value["field_name"] == ClioCustomFieldNames.NPA_INSPECT_SUMMARY.value
        ][0]
        return [
            {"value": incident.npa_inspect_summary, "id": npa_inspect_summary["id"]}
        ]

import os
from enum import Enum

BASE_DATA_DIR = os.environ.get("BASE_DATA_DIR", os.path.expanduser("~/dev/np-databridge/data"))


class GISFields(Enum):
    OBJECT_ID = "OBJECTID"
    LAST_EDITED_DATE = "LAST_EDITED_DATE"
    LAST_MODIFIED_DATE = "LAST_MODIFIED_DATE"
    CREATION_DATE = "CREATION_DATE"
    INCIDENT_NUMBER = "INCIDENT_NUMBER"
    PARCEL_ID = "PARCEL_ID"
    CITY_FILE_NO = "Case_Number"
    SUB_DISTRICT = "ce_name"
    NPA_INSPECT_SUMMARY = "NPA_Inspect_Summary"
    COURT_STATUS = "Court_Status"
    LOCATION = "ADDRESS1"
    NEXT_COURT_DATE = "NextCourtDate"
    PROPERTY_OWNER = "Court_Property_Owner"
    DEFENDENT = "AntiNeglect_Plans_App_By"
    CIVIL_WARRANT = "CivilWarrant"
    LATEST_COURT_NOTES = "BoardUp_Notes"
    COURT_NOTES = "CourtNotes"
    LITIGATION_HISTORY_INCIDENT_NUMBER = "Incident_Number"


class ClioCustomFieldNames(Enum):
    INCIDENT_NUMBER = "Incident Number"
    PARCEL_ID = "Parcel ID"
    CITY_FILE_NO = "City File Number"
    SUB_DISTRICT = "Subdistrict"
    NPA_INSPECT_SUMMARY = "NPA Inspection Summary"
    COURT_STATUS = "Court Status"
    LOCATION = "Location"
    NEXT_COURT_DATE = "Next Court Date"
    PROPERTY_OWNER = "Property Owner"
    DEFENDENT = "Defendent"
    CIVIL_WARRANT = "Civil Warrant"
    DOCUMENT_EXTERNAL_ID_FIELD = "GIS_ID"
    LONGITUDE = "GIS Latitude"
    LATITUDE = "GIS Longitude"
    GIS_OBJECT_ID = "GIS Object ID"


CLIO_GROUP_NAME = "NP Clinic"
CLIO_PRACTICE_AREA = "Neighborhood Preservation Test"
CLIO_CLIENT_NAME = "NP Clinic Client Test"
CLIO_CUSTOM_FIELDS = [
    {
        "name": ClioCustomFieldNames.INCIDENT_NUMBER.value,
        "field_type": "text_line",
        "parent_type": "Matter",
    },
    {
        "name": ClioCustomFieldNames.PARCEL_ID.value,
        "field_type": "text_line",
        "parent_type": "Matter",
    },
    {
        "name": ClioCustomFieldNames.CITY_FILE_NO.value,
        "field_type": "text_line",
        "parent_type": "Matter",
    },
    {
        "name": ClioCustomFieldNames.SUB_DISTRICT.value,
        "field_type": "text_line",
        "parent_type": "Matter",
    },
    {
        "name": ClioCustomFieldNames.NPA_INSPECT_SUMMARY.value,
        "field_type": "text_line",
        "parent_type": "Matter",
    },
    {
        "name": ClioCustomFieldNames.COURT_STATUS.value,
        "field_type": "text_line",
        "parent_type": "Matter",
    },
    {
        "name": ClioCustomFieldNames.LOCATION.value,
        "field_type": "text_line",
        "parent_type": "Matter",
    },
    {
        "name": ClioCustomFieldNames.NEXT_COURT_DATE.value,
        "field_type": "date",
        "parent_type": "Matter",
    },
    {
        "name": ClioCustomFieldNames.PROPERTY_OWNER.value,
        "field_type": "text_line",
        "parent_type": "Matter",
    },
    {
        "name": ClioCustomFieldNames.DEFENDENT.value,
        "field_type": "text_line",
        "parent_type": "Matter",
    },
    {
        "name": ClioCustomFieldNames.CIVIL_WARRANT.value,
        "field_type": "text_line",
        "parent_type": "Matter",
    },
    {
        "name": ClioCustomFieldNames.LONGITUDE.value,
        "field_type": "text_line",
        "parent_type": "Matter",
    },
    {
        "name": ClioCustomFieldNames.LATITUDE.value,
        "field_type": "text_line",
        "parent_type": "Matter",
    },
    {
        "name": ClioCustomFieldNames.GIS_OBJECT_ID.value,
        "field_type": "numeric",
        "parent_type": "Matter",
    },
]

CLIO_API_URL = "https://app.clio.com/api/v4/"
CLIO_AUTH_URL = "https://app.clio.com/oauth/authorize"
CLIO_TOKEN_URL = "https://app.clio.com/oauth/token"

CLIO_CALLBACK_URL = os.environ.get(
    "CLIO_CALLBACK_URL", "https://cd8d7c188316.ngrok.io/callback"
)

CLIO_API_KEY = os.environ.get("CLIO_API_KEY")
CLIO_API_SECRET = os.environ.get("CLIO_API_SECRET")

GIS_HOST = "https://mapview.memphistn.gov"
GIS_FEATURE_SERVER_PATH = (
    "arcgis/rest/services/AGO_Code/Code_Memphis_Fights_Blight/FeatureServer"
)
GIS_ACTIVE_LITIGATION_TABLE_ID = "2"
GIS_LITIGATION_HISTORY_TABLE_ID = "7"

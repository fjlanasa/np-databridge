from utils.clio_client import ClioApiClient
from utils.data_bridge import DataBridge
from utils.constants import (
    CLIO_CALENDAR_NAME,
    CLIO_CLIENT_NAME,
    CLIO_CUSTOM_FIELDS,
    CLIO_GROUP_NAME,
    CLIO_PRACTICE_AREA,
)
from utils.logging import logger

## Authenticate
## Create Group
## Create Client
## Create Practice Area
## Create webhook
## Create custom fields
## Create matters

if __name__ == "__main__":
    api_client = ClioApiClient()
    data_bridge = DataBridge(clio_client=api_client)

    logger.info("Loading Clio entities")

    if data_bridge.clio_client is None:
        logger.info("Loading Clio client")
        contact = api_client.get_contact(name=CLIO_CLIENT_NAME)
        if contact is None:
            logger.info(
                f"Clio client {CLIO_CLIENT_NAME} not found. Creating new client."
            )
            contact = api_client.create_contact(name=CLIO_CLIENT_NAME).json()["data"]
        logger.info(f"Saving Clio client to {data_bridge.client_path}: {contact}")
        data_bridge.save_entity(data_bridge.client_path, contact, "client")

    if data_bridge.practice_area is None:
        logger.info("Loading Clio practice area")
        practice_area = api_client.get_practice_area(name=CLIO_PRACTICE_AREA)
        if practice_area is None:
            logger.info(
                f"Clio practice area {CLIO_PRACTICE_AREA} not found. Creating new practice area."
            )
            practice_area = api_client.create_practice_area(
                name=CLIO_PRACTICE_AREA
            ).json()["data"]
        logger.info(
            f"Saving Clio practice area to {data_bridge.practice_area_path}: {practice_area}"
        )
        data_bridge.save_entity(
            data_bridge.practice_area_path, practice_area, "practice_area"
        )

    if data_bridge.group is None:
        logger.info("Loading Clio group")
        group = api_client.get_group(name=CLIO_GROUP_NAME)
        if group is None:
            logger.info(f"Clio group {CLIO_GROUP_NAME} not found. Creating new group.")
            group = api_client.create_group(name=CLIO_GROUP_NAME).json()["data"]
        logger.info(f"Saving Clio group to {data_bridge.group_path}: {group}")
        data_bridge.save_entity(data_bridge.group_path, group, "group")

    if data_bridge.custom_fields is None:
        logger.info("Loading Clio custom fields")
        entity = []
        for field in CLIO_CUSTOM_FIELDS:
            custom_field = api_client.get_custom_fields(field["name"])
            if custom_field:
                logger.info(f"Found existing Clio custom field {custom_field}")
                custom_field["name"] = field["name"]
            else:
                logger.info(
                    f"Custom field {field} not found. Creating new custom field."
                )
                custom_field_res = api_client.create_custom_fields(
                    name=field["name"],
                    field_type=field["field_type"],
                    displayed=field.get("displayed"),
                    pick_list_options=field.get("picklist_options"),
                )
                custom_field = custom_field_res.json()["data"]
                custom_field["name"] = field["name"]
            entity.append(custom_field)
        logger.info(
            f"Saving Clio custom fields to {data_bridge.custom_fields_path}: {entity}"
        )
        data_bridge.save_entity(data_bridge.custom_fields_path, entity, "custom_fields")

    if data_bridge.clio_calendar is None:
        logger.info("Loading Clio calendar")
        calendars_json = api_client.get_calendars().json()
        calendar = None
        if calendars_json and calendars_json["data"]:
            calendars = [
                cal
                for cal in calendars_json["data"]
                if cal["name"] == CLIO_CALENDAR_NAME
            ]
            if calendars:
                calendar = calendars[0]
        if calendar is None:
            logger.info(
                f"Clio calendar {CLIO_CALENDAR_NAME} not found. Creating new calendar."
            )
            res = api_client.create_calendar(name=CLIO_CALENDAR_NAME)
            calendar = res.json()["data"]
        logger.info(
            f"Saving Clio calendar to {data_bridge.clio_calendar_path}: {calendar}"
        )
        data_bridge.save_entity(data_bridge.clio_calendar_path, calendar, "calendar")

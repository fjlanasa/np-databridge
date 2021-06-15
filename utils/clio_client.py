import os
from typing import Any, Callable
import requests
from oauthlib.oauth2.rfc6749.tokens import OAuth2Token
from requests_oauthlib import OAuth2Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utils.constants import (
    CLIO_AUTH_URL,
    CLIO_TOKEN_URL,
    CLIO_API_URL,
    CLIO_API_KEY,
    CLIO_API_SECRET,
    CLIO_CALLBACK_URL,
    BASE_DATA_DIR,
    ClioCustomFieldNames,
)


class AuthClient:
    def __init__(
        self,
        api_key=CLIO_API_KEY,
        api_secret=CLIO_API_SECRET,
        callback_url=CLIO_CALLBACK_URL,
        auth_data_dir=BASE_DATA_DIR,
        refresh_token_filename="refresh",
        access_token_filename="access",
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.callback_url = callback_url
        auth_data_dir = os.path.join(BASE_DATA_DIR, "auth")
        os.makedirs(auth_data_dir, exist_ok=True)
        self.refresh_token_path = os.path.join(auth_data_dir, refresh_token_filename)
        self.access_token_path = os.path.join(auth_data_dir, access_token_filename)
        self.access_token = None
        self.refresh_token = None
        self.load_tokens()
        retry_strategy = Retry(
            total=10,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.client = OAuth2Session(
            self.api_key,
            token={
                "access_token": self.access_token,
                "refresh_token": self.refresh_token,
            },
            auto_refresh_kwargs={
                "client_id": self.api_key,
                "client_secret": self.api_secret,
            },
            token_updater=self.save_tokens,
        )
        self.client.mount("https://", adapter)
        self.client.mount("http://", adapter)

    def get_authorization_url(self):
        oauth_client = OAuth2Session(
            client_id=self.api_key, redirect_uri=self.callback_url
        )
        url, _ = oauth_client.authorization_url(CLIO_AUTH_URL)
        return url

    def get_token(self, auth_response_url):
        oauth_client = OAuth2Session(
            client_id=self.api_key, redirect_uri=self.callback_url
        )
        token = oauth_client.fetch_token(
            CLIO_TOKEN_URL,
            client_secret=self.api_secret,
            authorization_response=auth_response_url,
        )
        return token

    def save_tokens(self, token: OAuth2Token):
        with open(self.access_token_path, "w") as f:
            f.write(token["access_token"])

        with open(self.refresh_token_path, "w") as f:
            f.write(token["refresh_token"])

    def load_tokens(self):
        try:
            with open(
                self.access_token_path,
            ) as f:
                self.access_token = f.read()

            with open(self.refresh_token_path) as f:
                self.refresh_token = f.read()

            return self.access_token, self.refresh_token
        except FileNotFoundError:
            return None, None


def take_one(func: Callable[..., requests.Response]):
    def inner(*args, **kwargs):
        res = func(*args, **kwargs)
        if res.status_code:
            body = res.json()
            if bool(body["data"]):
                return body["data"][0]
        return None

    return inner


class ClioApiClient:
    def __init__(self, api_url=CLIO_API_URL, oauth_client=AuthClient()):
        self.api_url = api_url
        self.oauth: AuthClient = oauth_client

    def create_webhook(self):
        pass

    @take_one
    def get_matter(
        self,
        group_id,
        civil_warrant_field_id=None,
        civil_warrant_value=None,
    ):
        params = {
            "group_id": group_id,
            "fields": "id,etag,updated_at,custom_field_values{id,etag,field_name,value}",
        }
        if civil_warrant_field_id and civil_warrant_value:
            civil_warrant_key = f"custom_field_values[{civil_warrant_field_id}]"
            params[civil_warrant_key] = civil_warrant_value
        url = os.path.join(
            self.api_url,
            "matters",
        )
        return self.oauth.client.get(url, params=params)

    def get_matters(
        self,
        group_id,
        updated_since=None,
    ):
        params = {
            "group_id": group_id,
            "fields": "id,etag,updated_at,custom_field_values{id,etag,field_name,value}",
        }
        if updated_since:
            params["updated_since"] = updated_since
        url = os.path.join(
            self.api_url,
            "matters",
        )
        return self.oauth.client.get(url, params=params)

    def get_matter_by_id(self, id):
        url = os.path.join(self.api_url, "matters", str(id))
        return self.oauth.client.get(url, params={"fields": "updated_at"})

    def create_matter(
        self, description, client_id, group_id, practice_area_id, custom_field_values=[]
    ):
        url = os.path.join(self.api_url, "matters")
        return self.oauth.client.post(
            url,
            json={
                "data": {
                    "client": {
                        "id": client_id,
                    },
                    "group": {"id": group_id},
                    "practice_area": {"id": practice_area_id},
                    "description": description,
                    "custom_field_values": custom_field_values,
                }
            },
        )

    def update_matter(self, id, data):
        url = os.path.join(self.api_url, "matters", str(id))
        return self.oauth.client.patch(url, json={"data": data})

    @take_one
    def get_custom_fields(self, name):
        url = os.path.join(self.api_url, "custom_fields")
        return self.oauth.client.get(
            url, params={"query": name, "parent_type": "Matter"} if name else None
        )

    def create_custom_fields(self, name, field_type="text_line", displayed="true"):
        url = os.path.join(self.api_url, "custom_fields")
        return self.oauth.client.post(
            url,
            json={
                "data": {
                    "name": name,
                    "field_type": field_type,
                    "parent_type": "Matter",
                    "displayed": displayed,
                }
            },
        )

    def get_custom_field(self, id):
        url = os.path.join(self.api_url + "custom_fields", str(id))
        return self.oauth.client.get(url)

    def get_practice_areas(self, name=None):
        url = os.path.join(self.api_url, "practice_areas")
        return self.oauth.client.get(url, params={"name": name} if name else None)

    def get_practice_area(self, id):
        url = os.path.join(self.api_url, "practice_areas", str(id))
        return self.oauth.client.get(url)

    @take_one
    def get_practice_area(self, name=None):
        url = os.path.join(self.api_url, "practice_areas")
        return self.oauth.client.get(url, params={"name": name})

    def create_practice_area(self, name=None, code=None):
        url = os.path.join(self.api_url, "practice_areas")
        return self.oauth.client.post(url, json={"data": {"name": name, "code": code}})

    def get_notes(self, matter_id, updated_since=None):
        url = os.path.join(self.api_url, "notes")
        return self.oauth.client.get(
            url,
            params={
                "matter_id": matter_id,
                "type": "Matter",
                "updated_since": updated_since,
                "fields": "id,etag,detail",
            },
        )

    def create_note(self, matter_id, detail, subject=None):
        url = os.path.join(self.api_url, "notes")
        return self.oauth.client.post(
            url,
            json={
                "data": {
                    "matter": {"id": matter_id},
                    "detail": detail,
                    "subject": subject,
                    "type": "Matter",
                }
            },
        )

    def get_document_by_id(self, id):
        url = os.path.join(self.api_url, "documents", str(id))
        return self.oauth.client.get(url)

    @take_one
    def get_document(self, matter_id=None, gis_id=None):
        url = os.path.join(self.api_url, "documents")
        return self.oauth.client.get(
            url,
            params={
                "matter_id": matter_id,
                "external_property_name": ClioCustomFieldNames.DOCUMENT_EXTERNAL_ID_FIELD.value,
                "external_property_value": gis_id,
            },
        )

    def upload_document(self, matter_id, gis_id, file_name, file_content):
        create_url = os.path.join(self.api_url, "documents")
        create_res = self.oauth.client.post(
            create_url,
            params={"fields": "id,latest_document_version{uuid,put_url,put_headers}"},
            json={
                "data": {
                    "name": file_name,
                    "parent": {"type": "Matter", "id": matter_id},
                    "external_properties": [
                        {
                            "name": ClioCustomFieldNames.DOCUMENT_EXTERNAL_ID_FIELD.value,
                            "value": gis_id,
                        }
                    ],
                }
            },
        ).json()["data"]
        document_id = create_res["id"]
        document_uuid = create_res["latest_document_version"]["uuid"]
        document_put_url = create_res["latest_document_version"]["put_url"]
        headers = {
            header["name"]: header["value"]
            for header in create_res["latest_document_version"]["put_headers"]
        }

        put_res = requests.put(
            document_put_url, headers=headers, files={"file": file_content}
        )
        patch_url = os.path.join(create_url, str(document_id))
        patch_res = self.oauth.client.patch(
            patch_url,
            params={"fields": "id,latest_document_version{fully_uploaded}"},
            json={"data": {"fully_uploaded": "true", "uuid": document_uuid}},
        )
        return patch_res

    def get_group_by_id(self, id):
        url = os.path.join(self.api_url, "groups", str(id))
        return self.oauth.client.get(url)

    @take_one
    def get_group(self, name=None):
        url = os.path.join(self.api_url, "groups")
        params = {"name": name} if name else None
        return self.oauth.client.get(url, params=params)

    def create_group(self, name):
        url = os.path.join(self.api_url, "groups")
        return self.oauth.client.post(url, json={"data": {"name": name}})

    @take_one
    def get_contact(self, name=None):
        url = os.path.join(self.api_url, "contacts")
        params = {"query": name}
        return self.oauth.client.get(url, params=params)

    def create_contact(self, name=None):
        url = os.path.join(self.api_url, "contacts")
        return self.oauth.client.post(
            url, json={"data": {"name": name, "type": "Company"}}
        )

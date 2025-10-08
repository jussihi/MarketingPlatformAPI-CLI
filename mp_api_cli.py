#!/usr/bin/env python3
import os
import sys
import time
import json
import argparse
from typing import Any, Dict, Iterable, Optional
import requests
from dotenv import load_dotenv



MP_API_BASE = os.environ.get("MP_API_BASE", "https://api.mailmailmail.net/v2.0")
load_dotenv()

class MPError(Exception):
    pass


class MarketingPlatformClient:
    def __init__(self, username: str, token: str, base_url: str = MP_API_BASE, timeout: int = 30):
        if not username or not token:
            raise ValueError("API username/token missing. Set MP_API_USERNAME and MP_API_TOKEN.")
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "Apiusername": username,
            "Apitoken": token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        self.timeout = timeout

    def _request(self, method: str, path: str, *,
                 params: Optional[Dict[str, Any]] = None,
                 json_body: Optional[Dict[str, Any]] = None,
                 max_retries: int = 5) -> Dict[str, Any]:
        """Request with basic 429 backoff and error handling."""
        url = f"{self.base_url}{path}"
        if debug:
            print(f"Request: {method} {url}")
            if params:
                print(f"Params: {params}")
            if json_body:
                print(f"Body: {json.dumps(json_body, indent=2)}")
        backoff = 1.0
        for attempt in range(1, max_retries + 1):
            resp = self.session.request(method, url, params=params, json=json_body, timeout=self.timeout)
            if resp.status_code == 429:
                # Simple exponential backoff for rate limiting (API limit: 240 req/min). :contentReference[oaicite:1]{index=1}
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            if 200 <= resp.status_code < 300:
                try:
                    return resp.json()
                except json.JSONDecodeError:
                    raise MPError(f"Non-JSON response from {url}")
            # Map common errors from docs. :contentReference[oaicite:2]{index=2}
            if resp.status_code in (400, 401, 403, 404, 409, 500):
                raise MPError(f"{resp.status_code} error from {url}: {resp.text}")
            # Unexpected—retry a couple of times
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
        raise MPError(f"Failed after {max_retries} attempts: {url}")

    # ---------- Lists ----------
    def get_lists(self, listid: int = 0, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        """GET /Lists?listid=&limit=&offset= — returns lists you own or can access."""
        params = {"listid": listid, "limit": limit, "offset": offset}
        return self._request("GET", "/Lists", params=params)

    def iter_lists(self, listid: int = 0, page_size: int = 100) -> Iterable[Dict[str, Any]]:
        offset = 0
        while True:
            data = self.get_lists(listid=listid, limit=page_size, offset=offset)  # :contentReference[oaicite:3]{index=3}
            items = data.get("data") or []
            if not items:
                break
            for item in items:
                yield item
            if len(items) < page_size:
                break
            offset += page_size

    def create_list(self, **fields) -> Dict[str, Any]:
        """
        POST /Lists — required: name, description.
        Optional fields include sender_name, sender_email, reply_email, company_* etc. :contentReference[oaicite:4]{index=4}
        """
        return self._request("POST", "/Lists", json_body=fields)

    # ---------- Stats ----------
    def get_opens(self, statid: int) -> Dict[str, Any]:
        """GET /Stats/GetOpens?statid=... — contacts who opened an email campaign. :contentReference[oaicite:5]{index=5}"""
        return self._request("GET", "/Stats/GetOpens", params={"statid": statid})

    def get_clicks(self, statid: int, count_only: bool = False, unique_only: bool = False) -> Dict[str, Any]:
        """GET /Stats/GetClicks — clicks for an email campaign. :contentReference[oaicite:6]{index=6}"""
        params = {"statid": statid, "count_only": str(count_only).lower(), "unique_only": str(unique_only).lower()}
        return self._request("GET", "/Stats/GetClicks", params=params)
    
    # ---------- Stats → Unsubscribes by List ----------
    def get_unsubscribes_by_list(
        self,
        listid: int,
        *,
        count_only: bool = False,
        search_type: str | None = None,         # one of: before, after, between, not, exact/exactly
        search_start_date: str | int | None = None,  # UNIX ts or date string per docs
        search_end_date: str | int | None = None,    # required if search_type='between'
        limit: int = 100,
        offset: int = 0,
    ) -> dict:
        """
        GET /Stats/GetUnsubscribesByList

        Fetch unsubscribed profiles for a list. Supports date filtering via search_* params.
        """
        if not listid:
            raise ValueError("listid is required")

        params: dict = {
            "listid": listid,
            "count_only": str(count_only).lower(),
            "limit": limit,
            "offset": offset,
        }
        if search_type:
            params["search_type"] = search_type
        if search_start_date is not None:
            params["search_start_date"] = search_start_date
        if search_end_date is not None:
            params["search_end_date"] = search_end_date

        return self._request("GET", "/Stats/GetUnsubscribesByList", params=params)

    # ---------- Send ----------
    def send_newsletter(self, newsletterid: int) -> Dict[str, Any]:
        """
        POST /Send/SendNewsletter — immediate send (use carefully). Endpoint name per docs. :contentReference[oaicite:7]{index=7}
        """
        return self._request("POST", "/Send/SendNewsletter", json_body={"newsletterid": newsletterid})

    def schedule_send_newsletter_to_list(self, newsletterid: int, listid: int, send_time_ts: Optional[int] = None) -> Dict[str, Any]:
        """
        POST /Send/ScheduleSendNewsletterToList — schedule a campaign to a specific list.
        If send_time_ts is None, platform may send ASAP depending on API behavior. :contentReference[oaicite:8]{index=8}
        """
        payload = {"newsletterid": newsletterid, "listid": listid}
        if send_time_ts is not None:
            payload["send_time"] = send_time_ts  # docs return “system dates are timestamps”. :contentReference[oaicite:9]{index=9}
        return self._request("POST", "/Send/ScheduleSendNewsletterToList", json_body=payload)
    
    def add_profile_to_list(
        self,
        listid: int,
        email_address: Optional[str] = None,
        mobile_number: Optional[str] = None,
        mobile_prefix: Optional[str] = None,
        data_fields: Optional[list] = None,
        confirmed: bool = True,
        mobile_confirmed: bool = True,
        add_to_autoresponders: bool = False,
    ) -> Dict[str, Any]:
        """
        POST /Profiles — AddProfileToList.
        You must provide either email_address OR (mobile_number + mobile_prefix).
        data_fields: list of {"fieldid": <int or str>, "field_value": <any>}.
        """
        if not listid:
            raise ValueError("listid is required")
        if not email_address and not (mobile_number and mobile_prefix):
            raise ValueError("Provide email_address OR mobile_number+mobile_prefix")

        payload: Dict[str, Any] = {
            "listid": listid,
            "data_fields": data_fields or [],
            "confirmed": confirmed,
            "mobile_confirmed": mobile_confirmed,
            "add_to_autoresponders": add_to_autoresponders,
        }
        if email_address:
            payload["email_address"] = email_address
        else:
            payload["mobile_number"] = mobile_number
            payload["mobile_prefix"] = mobile_prefix

        return self._request("POST", "/Profiles", json_body=payload)  # docs use /Profiles for POST

    def update_profile(self, profileid: int, data_fields: list) -> Dict[str, Any]:
        """
        PUT /Profiles/UpdateProfile — update a profile's data fields.
        data_fields: list of {"fieldid": <int or str>, "field_value": <any>}
        """
        if not profileid:
            raise ValueError("profileid is required")
        if not data_fields:
            raise ValueError("data_fields cannot be empty")

        payload = {"profileid": profileid, "data_fields": data_fields}
        return self._request("PUT", "/Profiles/UpdateProfile", json_body=payload)

    def get_profiles_by_list(self, listid: int, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        """GET /Profiles/GetProfilesByList — profiles on a list."""
        params = {"listid": listid, "limit": limit, "offset": offset}
        return self._request("GET", "/Profiles/GetProfilesByList", params=params)

    def get_profiles_from_segment(self, segmentid: int, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        """GET /Profiles/GetProfilesFromSegment — profiles in a segment."""
        params = {"segmentid": segmentid, "limit": limit, "offset": offset}
        return self._request("GET", "/Profiles/GetProfilesFromSegment", params=params)

    # ---------- Segments ----------
    def get_segments(self, segmentid: int = 0, listid: Optional[int] = None, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        """
        GET /Segments — fetch segments (optionally filter by listid or a specific segmentid).
        """
        params: Dict[str, Any] = {"segmentid": segmentid, "limit": limit, "offset": offset}
        if listid is not None:
            params["listid"] = listid
        return self._request("GET", "/Segments", params=params)

    def create_segment(self, name: str, rules: Dict[str, Any], connector: str = "and") -> Dict[str, Any]:
        """
        POST /Segments — create a segment.
        rules should follow the documented structure, e.g.:
        {"Segments": [{"listids":[205], "rules":[{"ruleName":"emailaddress","ruleOperator":"=","ruleValues":["john@ex.com"]}]}]}
        """
        if not name:
            raise ValueError("name is required")
        if not rules:
            raise ValueError("rules are required")
        payload = {"name": name, "rules": rules, "connector": connector}
        return self._request("POST", "/Segments", json_body=payload)

    def edit_segment(self, segmentid: int, name: str, rules: Dict[str, Any], connector: str = "and") -> Dict[str, Any]:
        """PUT /Segments — edit an existing segment."""
        if not segmentid:
            raise ValueError("segmentid is required")
        payload = {"segmentid": segmentid, "name": name, "rules": rules, "connector": connector}
        return self._request("PUT", "/Segments", json_body=payload)

    def delete_segment(self, segmentid: int) -> Dict[str, Any]:
        """DELETE /Segments — delete a segment."""
        if not segmentid:
            raise ValueError("segmentid is required")
        return self._request("DELETE", "/Segments", params={"segmentid": segmentid})
    
    def load_data_fields(
        self,
        fieldid: int | None = None,
        load_lists: bool = False,
        limit: int = 10,
        offset: int = 0,
    ):
        """
        GET /DataFields
        Loads data fields. Optionally filter by fieldid and include linked lists.
        """
        params = {
            "fieldid": fieldid if fieldid is not None else "",
            "load_lists": str(load_lists).lower(),
            "limit": limit,
            "offset": offset,
        }
        return self._request("GET", "/DataFields", params=params)

    def create_data_field(
        self,
        name: str,
        field_type: str,
        *,
        default_value: str | None = None,
        settings: dict | None = None,
    ):
        """
        POST /DataFields
        field_type in: text / textarea / number / dropdown / checkbox / radiobutton / date
        """
        if not name or not field_type:
            raise ValueError("name and field_type are required")
        payload = {"name": name, "field_type": field_type}
        if default_value is not None:
            payload["default_value"] = default_value
        if settings is not None:
            payload["settings"] = settings
        return self._request("POST", "/DataFields", json_body=payload)

    def update_data_field(
        self,
        *,
        fieldid: int | None = None,
        name: str | None = None,
        default_value: str | None = None,
        settings: dict | None = None,
    ):
        """
        PUT /DataFields
        You must provide either fieldid or name (doc allows either).
        """
        if fieldid is None and not name:
            raise ValueError("Provide fieldid or name")
        payload: dict = {}
        if fieldid is not None:
            payload["fieldid"] = fieldid
        if name:
            payload["name"] = name
        if default_value is not None:
            payload["default_value"] = default_value
        if settings is not None:
            payload["settings"] = settings
        return self._request("PUT", "/DataFields", json_body=payload)

    def delete_data_field(self, fieldid: int):
        """DELETE /DataFields — delete the field by id."""
        if not fieldid:
            raise ValueError("fieldid is required")
        return self._request("DELETE", "/DataFields", params={"fieldid": fieldid})

    # ---------- Data Fields linked to Lists ----------
    def lists_add_data_fields(self, listid: int, field_ids: list[int]):
        """POST /Lists/AddDataFieldsToList — link existing data fields to a list."""
        if not listid or not field_ids:
            raise ValueError("listid and field_ids are required")
        payload = {"listid": listid, "data_fields": field_ids}
        return self._request("POST", "/Lists/AddDataFieldsToList", json_body=payload)

    def lists_remove_data_fields(self, listid: int, field_ids: list[int]):
        """POST /Lists/RemoveDataFieldsFromList — unlink fields from a list."""
        if not listid or not field_ids:
            raise ValueError("listid and field_ids are required")
        payload = {"listid": listid, "data_fields": field_ids}
        return self._request("POST", "/Lists/RemoveDataFieldsFromList", json_body=payload)

    def lists_get_data_fields(self, listid: int, field_type: str | None = None, limit: int = 100, offset: int = 0):
        """GET /Lists/GetDataFields — fetch data fields on a given list."""
        params = {"listid": listid, "limit": limit, "offset": offset}
        if field_type:
            params["field_type"] = field_type
        return self._request("GET", "/Lists/GetDataFields", params=params)

    # ---------- Profiles → Data Fields ----------
    def profiles_load_profile_data_fields(
        self,
        profileid: int | None = None,
        listid: int | None = None,
        email: str | None = None,
        mobile_number: str | None = None,
        mobile_prefix: str | None = None,
    ):
        """
        GET /Profiles/LoadProfileDataFields
        Provide one of: profileid OR (listid + email) OR (listid + mobile_number + mobile_prefix)
        """
        params: dict = {}
        if profileid is not None:
            params["profileid"] = profileid
        if listid is not None:
            params["listid"] = listid
        if email:
            params["email_address"] = email
        if mobile_number:
            params["mobile_number"] = mobile_number
        if mobile_prefix:
            params["mobile_prefix"] = mobile_prefix
        return self._request("GET", "/Profiles/LoadProfileDataFields", params=params)
    
    def get_unsubscribed_profiles(
        self,
        *,
        listid: int | None = None,
        count_only: bool = False,
        search_type: str | None = None,         # before / after / between / not / exact
        search_start_date: str | int | None = None,
        search_end_date: str | int | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict:
        """
        GET /Profiles/GetUnsubscribedProfiles
        Returns unsubscribed profiles (globally or filtered by list/date range).
        """
        params: dict = {
            "count_only": str(count_only).lower(),
            "limit": limit,
            "offset": offset,
        }
        if listid is not None:
            params["listid"] = listid
        if search_type:
            params["search_type"] = search_type
        if search_start_date is not None:
            params["search_start_date"] = search_start_date
        if search_end_date is not None:
            params["search_end_date"] = search_end_date

        return self._request("GET", "/Profiles/GetUnsubscribedProfiles", params=params)
    
    # ---------- Profiles → SMS Unsubscribed ----------
    def get_profiles_sms_unsubscribed(
        self,
        listid: int,
        date: str | int,
        *,
        type: str = "on",          # on / before / after
        limit: int = 100,          # 100 default, 1000 max
        offset: int = 0,
    ) -> dict:
        """
        GET /Profiles/GetProfilesSMSUnsubscribed
        Required: listid, date. Optional: type (on|before|after), limit (<=1000), offset.
        `date` can be a UNIX timestamp or a date string accepted by the API (e.g., '2024-06-01').
        """
        if not listid:
            raise ValueError("listid is required")
        if date is None or date == "":
            raise ValueError("date is required")

        t = type.lower() if isinstance(type, str) else type
        if t not in {"on", "before", "after"}:
            raise ValueError("type must be one of: on, before, after")

        if limit <= 0:
            raise ValueError("limit must be > 0")
        if limit > 1000:
            limit = 1000  # cap to API maximum

        params = {
            "listid": listid,
            "date": date,
            "type": t,
            "limit": limit,
            "offset": offset,
        }
        return self._request("GET", "/Profiles/GetProfilesSMSUnsubscribed", params=params)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="MarketingPlatform API v2.0 CLI")
    p.add_argument("--debug", action="store_true", help="Print API requests for debugging")
    sub = p.add_subparsers(dest="cmd", required=True)

    # lists
    p_lists = sub.add_parser("lists", help="List your lists")
    p_lists.add_argument("--listid", type=int, default=0)
    p_lists.add_argument("--limit", type=int, default=100)

    p_make_list = sub.add_parser("create-list", help="Create a list")
    p_make_list.add_argument("--name", required=True)
    p_make_list.add_argument("--description", required=True)
    p_make_list.add_argument("--sender-name")
    p_make_list.add_argument("--sender-email")
    p_make_list.add_argument("--reply-email")
    p_make_list.add_argument("--company-name")
    p_make_list.add_argument("--company-email")
    p_make_list.add_argument("--company-address")
    p_make_list.add_argument("--company-phone")
    p_make_list.add_argument("--company-domain")

    # stats
    p_opens = sub.add_parser("opens", help="Get opens by campaign statid")
    p_opens.add_argument("statid", type=int)

    p_clicks = sub.add_parser("clicks", help="Get clicks by campaign statid")
    p_clicks.add_argument("statid", type=int)
    p_clicks.add_argument("--count-only", action="store_true")
    p_clicks.add_argument("--unique-only", action="store_true")

    # send
    p_send = sub.add_parser("send-now", help="Send a newsletter immediately")
    p_send.add_argument("newsletterid", type=int)

    p_sched = sub.add_parser("schedule-list", help="Schedule a newsletter to a list")
    p_sched.add_argument("newsletterid", type=int)
    p_sched.add_argument("listid", type=int)
    p_sched.add_argument("--send-time-ts", type=int, help="Unix timestamp when to send")

    # 🔹 Profiles
    p_p_add = sub.add_parser("pf-add", help="Add a profile to a list (email or mobile required)")
    p_p_add.add_argument("listid", type=int)
    p_p_add.add_argument("--email", dest="email_address")
    p_p_add.add_argument("--mobile", dest="mobile_number")
    p_p_add.add_argument("--prefix", dest="mobile_prefix", help="Country dialing code, e.g. 45")
    p_p_add.add_argument("--field", dest="fields", action="append",
                         help='Data field as FIELDID=VALUE (repeatable), e.g. 2="Jane Doe"')
    p_p_add.add_argument("--unconfirmed", action="store_true", help="Create as unconfirmed (sends confirmation email)")
    p_p_add.add_argument("--sms-unsubscribed", action="store_true", help="Create with mobile unsubscribed status")
    p_p_add.add_argument("--add-to-autoresponders", action="store_true")

    p_p_upd = sub.add_parser("pf-update", help="Update a profile's data fields")
    p_p_upd.add_argument("profileid", type=int)
    p_p_upd.add_argument("--field", dest="fields", required=True, action="append",
                         help='Data field as FIELDID=VALUE (repeatable)')

    p_p_list = sub.add_parser("pfs-by-list", help="List profiles on a list")
    p_p_list.add_argument("listid", type=int)
    p_p_list.add_argument("--limit", type=int, default=100)
    p_p_list.add_argument("--offset", type=int, default=0)

    p_p_seg = sub.add_parser("pfs-from-segment", help="List profiles from a segment")
    p_p_seg.add_argument("segmentid", type=int)
    p_p_seg.add_argument("--limit", type=int, default=100)
    p_p_seg.add_argument("--offset", type=int, default=0)

    # 🔹 Segments
    p_s_get = sub.add_parser("segments", help="List segments (optionally filter by list)")
    p_s_get.add_argument("--segmentid", type=int, default=0)
    p_s_get.add_argument("--listid", type=int)
    p_s_get.add_argument("--limit", type=int, default=100)
    p_s_get.add_argument("--offset", type=int, default=0)

    p_s_create = sub.add_parser("segment-create", help="Create a segment")
    p_s_create.add_argument("name")
    p_s_create.add_argument("--rules-json", required=True,
                            help="Rules JSON (see docs examples).")

    p_s_edit = sub.add_parser("segment-edit", help="Edit a segment")
    p_s_edit.add_argument("segmentid", type=int)
    p_s_edit.add_argument("name")
    p_s_edit.add_argument("--rules-json", required=True)
    p_s_edit.add_argument("--connector", default="and")

    p_s_delete = sub.add_parser("segment-delete", help="Delete a segment")
    p_s_delete.add_argument("segmentid", type=int)

    p_df_load = sub.add_parser("df-load", help="Load data fields (optionally one by id)")
    p_df_load.add_argument("--fieldid", type=int)
    p_df_load.add_argument("--load-lists", action="store_true")
    p_df_load.add_argument("--limit", type=int, default=10)
    p_df_load.add_argument("--offset", type=int, default=0)

    p_df_create = sub.add_parser("df-create", help="Create a data field")
    p_df_create.add_argument("name")
    p_df_create.add_argument("field_type", choices=["text","textarea","number","dropdown","checkbox","radiobutton","date"])
    p_df_create.add_argument("--default")
    p_df_create.add_argument("--settings-json", help="JSON settings per docs")

    p_df_update = sub.add_parser("df-update", help="Update a data field (by id or name)")
    p_df_update.add_argument("--fieldid", type=int)
    p_df_update.add_argument("--name")
    p_df_update.add_argument("--default")
    p_df_update.add_argument("--settings-json", help="JSON settings per docs")

    p_df_delete = sub.add_parser("df-delete", help="Delete a data field by id")
    p_df_delete.add_argument("fieldid", type=int)

    # Data Fields ↔ Lists
    p_df_add_to_list = sub.add_parser("list-add-fields", help="Link data fields to a list")
    p_df_add_to_list.add_argument("listid", type=int)
    p_df_add_to_list.add_argument("field_ids", nargs="+", type=int)

    p_df_rm_from_list = sub.add_parser("list-remove-fields", help="Unlink data fields from a list")
    p_df_rm_from_list.add_argument("listid", type=int)
    p_df_rm_from_list.add_argument("field_ids", nargs="+", type=int)

    p_df_list_get = sub.add_parser("list-get-fields", help="Get data fields of a list")
    p_df_list_get.add_argument("listid", type=int)
    p_df_list_get.add_argument("--field-type")
    p_df_list_get.add_argument("--limit", type=int, default=100)
    p_df_list_get.add_argument("--offset", type=int, default=0)

    # Profiles → Data Fields
    p_prof_df = sub.add_parser("profile-fields", help="Load a profile's data fields")
    p_prof_df.add_argument("--profileid", type=int)
    p_prof_df.add_argument("--listid", type=int)
    p_prof_df.add_argument("--email")
    p_prof_df.add_argument("--mobile")
    p_prof_df.add_argument("--prefix")

    p_unsubs = sub.add_parser(
        "unsubs-by-list",
        help="Fetch unsubscribes for a list (optionally filtered by date)"
    )
    p_unsubs.add_argument("listid", type=int)
    p_unsubs.add_argument("--count-only", action="store_true")
    p_unsubs.add_argument("--search-type",
                          choices=["before", "after", "between", "not", "exact", "exactly"])
    p_unsubs.add_argument("--start", dest="search_start_date",
                          help="Start date/timestamp (e.g. 1578618000 or 2020-01-10)")
    p_unsubs.add_argument("--end", dest="search_end_date",
                          help="End date/timestamp (required when --search-type=between)")
    p_unsubs.add_argument("--limit", type=int, default=100)
    p_unsubs.add_argument("--offset", type=int, default=0)

    p_prof_unsub = sub.add_parser(
        "unsubs-profs",
        help="Get unsubscribed profiles (optionally by list and date)"
    )
    p_prof_unsub.add_argument("--listid", type=int, help="Limit to a specific list ID")
    p_prof_unsub.add_argument("--count-only", action="store_true")
    p_prof_unsub.add_argument("--search-type",
                              choices=["before", "after", "between", "not", "exact", "exactly"])
    p_prof_unsub.add_argument("--start", dest="search_start_date",
                              help="Start date/timestamp (e.g. 1578618000 or 2020-01-10)")
    p_prof_unsub.add_argument("--end", dest="search_end_date",
                              help="End date/timestamp (for between searches)")
    p_prof_unsub.add_argument("--limit", type=int, default=100)
    p_prof_unsub.add_argument("--offset", type=int, default=0)

    p_prof_sms_unsub = sub.add_parser(
        "sms-unsubs-profs",
        help="Get SMS-unsubscribed profiles for a list (requires listid and date)"
    )
    p_prof_sms_unsub.add_argument("listid", type=int, help="List ID")
    p_prof_sms_unsub.add_argument("date", help='Date/timestamp, e.g. "2024-06-01" or 1717200000')
    p_prof_sms_unsub.add_argument("--type", choices=["on", "before", "after"], default="on",
                                  help='How to compare against --date (default: on)')
    p_prof_sms_unsub.add_argument("--limit", type=int, default=100, help="Max 1000")
    p_prof_sms_unsub.add_argument("--offset", type=int, default=0)

    return p

def _parse_fields(pairs: Optional[list]) -> list:
    """Convert ['2=Jane Doe','7=Helsinki'] → [{'fieldid':'2','field_value':'Jane Doe'}, ...]."""
    out = []
    if not pairs:
        return out
    for item in pairs:
        if "=" not in item:
            raise ValueError(f"Invalid --field '{item}'. Use FIELDID=VALUE")
        k, v = item.split("=", 1)
        out.append({"fieldid": k.strip(), "field_value": v})
    return out

def _maybe_json(s: str | None) -> dict | None:
    if not s:
        return None
    import json as _json
    return _json.loads(s)

def main(argv=None):
    global debug
    argv = argv or sys.argv[1:]
    parser = build_parser()
    args = parser.parse_args(argv)

    debug = args.debug

    username = os.environ.get("MP_API_USERNAME")
    token = os.environ.get("MP_API_TOKEN")
    client = MarketingPlatformClient(username, token)

    if args.cmd == "lists":
        out = list(client.iter_lists(listid=args.listid, page_size=args.limit))
    elif args.cmd == "create-list":
        fields = {
            "name": args.name,
            "description": args.description,
            "sender_name": args.sender_name,
            "sender_email": args.sender_email,
            "reply_email": args.reply_email,
            "company_name": args.company_name,
            "company_email": args.company_email,
            "company_address": args.company_address,
            "company_phone": args.company_phone,
            "company_domain": args.company_domain,
        }
        # Drop None values
        fields = {k: v for k, v in fields.items() if v is not None}
        out = client.create_list(**fields)
    elif args.cmd == "opens":
        out = client.get_opens(args.statid)
    elif args.cmd == "clicks":
        out = client.get_clicks(args.statid, count_only=args.count_only, unique_only=args.unique_only)
    elif args.cmd == "send-now":
        out = client.send_newsletter(args.newsletterid)
    elif args.cmd == "schedule-list":
        out = client.schedule_send_newsletter_to_list(args.newsletterid, args.listid, args.send_time_ts)
    # ---------- Profiles ----------
    elif args.cmd == "pf-add":
        fields = _parse_fields(args.fields)
        out = client.add_profile_to_list(
            listid=args.listid,
            email_address=args.email_address,
            mobile_number=args.mobile_number,
            mobile_prefix=args.mobile_prefix,
            data_fields=fields,
            confirmed=not args.unconfirmed,
            mobile_confirmed=not args.sms_unsubscribed,
            add_to_autoresponders=args.add_to_autoresponders,
        )

    elif args.cmd == "pf-update":
        fields = _parse_fields(args.fields)
        out = client.update_profile(args.profileid, fields)

    elif args.cmd == "pfs-by-list":
        out = client.get_profiles_by_list(args.listid, limit=args.limit, offset=args.offset)

    elif args.cmd == "pfs-from-segment":
        out = client.get_profiles_from_segment(args.segmentid, limit=args.limit, offset=args.offset)

    # ---------- Segments ----------
    elif args.cmd == "segments":
        out = client.get_segments(segmentid=args.segmentid, listid=getattr(args, "listid", None),
                                  limit=args.limit, offset=args.offset)

    elif args.cmd == "segment-create":
        rules = json.loads(args.rules_json)
        out = client.create_segment(args.name, rules)

    elif args.cmd == "segment-edit":
        rules = json.loads(args.rules_json)
        out = client.edit_segment(args.segmentid, args.name, rules, connector=args.connector)

    elif args.cmd == "segment-delete":
        out = client.delete_segment(args.segmentid)

    elif args.cmd == "df-load":
        out = client.load_data_fields(
            fieldid=args.fieldid, load_lists=args.load_lists, limit=args.limit, offset=args.offset
        )

    elif args.cmd == "df-create":
        out = client.create_data_field(
            name=args.name,
            field_type=args.field_type,
            default_value=getattr(args, "default", None),
            settings=_maybe_json(getattr(args, "settings_json", None)),
        )

    elif args.cmd == "df-update":
        out = client.update_data_field(
            fieldid=getattr(args, "fieldid", None),
            name=getattr(args, "name", None),
            default_value=getattr(args, "default", None),
            settings=_maybe_json(getattr(args, "settings_json", None)),
        )

    elif args.cmd == "df-delete":
        out = client.delete_data_field(args.fieldid)

    elif args.cmd == "list-add-fields":
        out = client.lists_add_data_fields(args.listid, args.field_ids)

    elif args.cmd == "list-remove-fields":
        out = client.lists_remove_data_fields(args.listid, args.field_ids)

    elif args.cmd == "list-get-fields":
        out = client.lists_get_data_fields(args.listid, field_type=args.field_type, limit=args.limit, offset=args.offset)

    elif args.cmd == "profile-fields":
        out = client.profiles_load_profile_data_fields(
            profileid=getattr(args, "profileid", None),
            listid=getattr(args, "listid", None),
            email=getattr(args, "email", None),
            mobile_number=getattr(args, "mobile", None),
            mobile_prefix=getattr(args, "prefix", None),
        )
    
    elif args.cmd == "unsubs-by-list":
        out = client.get_unsubscribes_by_list(
            args.listid,
            count_only=args.count_only,
            search_type=getattr(args, "search_type", None),
            search_start_date=getattr(args, "search_start_date", None),
            search_end_date=getattr(args, "search_end_date", None),
            limit=args.limit,
            offset=args.offset,
        )

    elif args.cmd == "unsubs-profs":
        out = client.get_unsubscribed_profiles(
            listid=getattr(args, "listid", None),
            count_only=args.count_only,
            search_type=getattr(args, "search_type", None),
            search_start_date=getattr(args, "search_start_date", None),
            search_end_date=getattr(args, "search_end_date", None),
            limit=args.limit,
            offset=args.offset,
        )
    
    elif args.cmd == "sms-unsubs-profs":
        out = client.get_profiles_sms_unsubscribed(
            args.listid,
            args.date,
            type=args.type,
            limit=args.limit,
            offset=args.offset,
        )

    else:
        parser.error("Unknown command")

    print(json.dumps(out, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    try:
        main()
    except Exception as ex:
        print(f"Error: {ex}")

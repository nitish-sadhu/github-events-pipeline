#!/Users/krishnasadhu/github-events-analytics/.venv/bin/python
import pandas as pd
import pyarrow as pa
from google.cloud import storage, bigquery
from datetime import datetime
import json
import hashlib
import logging
import argparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_bucket(client, bucket):
    client.create_bucket(bucket)

    return None

def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--date", required=True)
    parser.add_argument("--hour", required=True)

    args = parser.parse_args()

    date = args.date
    hour = args.hour

    return date, hour

def extract_from_date(date: str):
    year = pd.Timestamp(date).strftime("%Y")
    month = pd.Timestamp(date).strftime("%m")
    day = pd.Timestamp(date).strftime("%d")

    return year, month, day


def create_bigquery_client():
    return bigquery.Client(project="github-events-analytics")

def create_storage_client():
    #logger.info("___CREATING_STORAGE_CLIENT___")
    client =  storage.Client(project="github-events-analytics")
    #logger.info(f"___{client}___")
    return client

def create_surrogate_id(record):
    record_str = json.dumps(record, sort_keys=True)
    return hashlib.md5(record_str.encode()).hexdigest()

def normalize_record(record):
    norm_record = {
        "surrogate_id": str(create_surrogate_id(record)),
        "id": str(record.get("id")),
        "type": str(record.get("type")),
        "actor": normalize_actor(record.get("actor"), record),
        "repo": normalize_repo(record.get("repo") or record.get("repository")),
        "public": bool(record.get("public")),
        "created_at": parse_time(record.get("created_at")),
        "org": normalize_org(record.get("org"))
    }

    return norm_record

def normalize_actor(actor, record):
    if actor is None:
        return None

    if isinstance(actor, str):
        return {
            "id": None,
            "login": actor,
            "display_login": actor,
            "gravatar_id": record.get("actor_attributes").get("gravatar_id"),
            "url": None,
            "avatar_url": None,
        }
    return {
        "id": str(actor.get("id")),
        "login": str(actor.get("login")),
        "display_login": str(actor.get("display_login")),
        "gravatar_id": str(actor.get("gravatar_id")),
        "url": str(actor.get("url")),
        "avatar_url": str(actor.get("avatar_url"))
    }


def normalize_repo(repo):
    if repo is None:
        return None
    return {
        "id": str(repo.get("id")),
        "name": str(repo.get("name")),
        "url": str(repo.get("url"))
    }

def normalize_org(org):
    if org is None:
        return None
    return {
        "id": str(org.get("id")),
        "login": str(org.get("login")),
        "gravatar_id": str(org.get("gravatar_id")),
        "url": str(org.get("url")),
        "avatar_url": str(org.get("avatar_url"))
    }


def parse_time(ts):

    if not ts:
        return None

    # Case 1: ISO format (new data)
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        pass

    # Case 2: old GitHub format
    try:
        return datetime.strptime(ts, "%Y/%m/%d %H:%M:%S %z")
    except ValueError:
        pass

    # fallback
    return None

def get_pa_schema():
    schema = pa.schema([
        ("surrogate_id", pa.string()),
        ("id", pa.string()),
        ("type", pa.string()),
        ("actor", pa.struct([
            ("id", pa.string()),
            ("login", pa.string()),
            ("display_login", pa.string()),
            ("gravatar_id", pa.string()),
            ("url", pa.string()),
            ("avatar_url", pa.string())
        ])
         ),
        ("repo", pa.struct([
            ("id", pa.string()),
            ("name", pa.string()),
            ("url", pa.string()),
        ])
         ),
        ("public", pa.bool_()),
        ("created_at", pa.timestamp("ms")),
        ("org", pa.struct([
            ("id", pa.string()),
            ("login", pa.string()),
            ("gravatar_id", pa.string()),
            ("url", pa.string()),
            ("avatar_url", pa.string())
        ])
         )
    ])

    return schema


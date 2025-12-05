import os
import json
import requests
import gspread
from google.oauth2.service_account import Credentials


# -----------------------------
# CloseCRM Setup
# -----------------------------

CLOSE_API_KEY = os.getenv("CLOSE_API_KEY")
CLOSE_API_URL = "https://api.close.com/api/v1/data/search/"


# -----------------------------
# Google Sheets Setup
# -----------------------------

GOOGLE_SA_FILE = "service-account.json"

# Load service account JSON correctly
with open(GOOGLE_SA_FILE, "r") as f:
    service_account_info = json.load(f)

print("DEBUG: Loaded service account email →", service_account_info.get("client_email"))

credentials = Credentials.from_service_account_info(
    service_account_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)

gc_global = gspread.authorize(credentials)

SHEET_ID = os.getenv("SHEET_ID")
TAB_NAME = "Sheet11"  # Google Sheets tab name


def authorize_google():
    """Return an authorized gspread client."""
    # We ALREADY loaded the JSON above, no need to reload or json.loads anything
    return gc_global


# -----------------------------
# Define all filters + cells here
# -----------------------------

METRICS = [
    {
        "name": "New leads this week",
        "cell": "B2",
        "filter": {
            "query": {
                "type": "and",
                "queries": [
                    {"type": "field", "field": "lead.status_label", "value": "New"},
                    {"type": "daterange", "field": "lead.created_at", "range": "this_week"}
                ]
            },
            "type": "lead",
            "limit": 200
        }
    },
    {
        "name": "Lost leads this week",
        "cell": "B3",
        "filter": {
            "query": {
                "type": "and",
                "queries": [
                    {"type": "field", "field": "lead.status_label", "value": "Lost"},
                    {"type": "daterange", "field": "lead.updated_at", "range": "this_week"}
                ]
            },
            "type": "lead",
            "limit": 200
        }
    },
    {
        "name": "Unassigned leads",
        "cell": "B4",
        "filter": {
            "query": {
                "type": "and",
                "queries": [
                    {"type": "field", "field": "lead.owner_id", "exists": False}
                ]
            },
            "type": "lead",
            "limit": 200
        }
    }
]


# -----------------------------
# CloseCRM API Helper
# -----------------------------

def run_close_filter(json_filter):
    """Run JSON filter query against CloseCRM."""
    response = requests.post(
        CLOSE_API_URL,
        auth=(CLOSE_API_KEY, ""),
        json=json_filter,
        headers={"Content-Type": "application/json"}
    )

    if response.status_code != 200:
        print("❌ CloseCRM error:", response.text)
        return None

    data = response.json().get("data", [])
    return len(data)


# -----------------------------
# Main Sync Logic
# -----------------------------

def main():
    print("🚀 Running Multi-Metric CloseCRM → Google Sheets Sync...")
    
    gc = authorize_google()
    sheet = gc.open_by_key(SHEET_ID).worksheet(TAB_NAME)

    for metric in METRICS:
        name = metric["name"]
        cell = metric["cell"]
        json_filter = metric["filter"]

        print(f"\n📡 Running filter: {name}...")

        count = run_close_filter(json_filter)

        if count is None:
            print(f"❌ Skipping '{name}' due to API error.")
            continue

        print(f"📊 {name}: {count}")
        print(f"📝 Writing to {TAB_NAME} cell {cell}...")

        sheet.update_acell(cell, count)

    print("\n✅ Sync complete! All metrics updated.")


if __name__ == "__main__":
    main()

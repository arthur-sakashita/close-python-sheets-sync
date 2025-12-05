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

with open(GOOGLE_SA_FILE, "r") as f:
    service_account_info = json.load(f)

credentials = Credentials.from_service_account_info(
    service_account_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)

gc = gspread.authorize(credentials)
SHEET_ID = os.getenv("SHEET_ID")
SHEET_NAME = "Sheet11"


# =============================================================================
# 🔍 JSON FILTER (REMOVE _limit AND cursor)
# =============================================================================
BLOOMFIRE_FILTER = {
    "query": {
        "negate": False,
        "queries": [
            {
                "negate": False,
                "object_type": "lead",
                "type": "object_type"
            },
            {
                "negate": False,
                "queries": [
                    {
                        "negate": False,
                        "queries": [
                            {
                                "negate": False,
                                "queries": [
                                    {
                                        "condition": {
                                            "type": "term",
                                            "values": [
                                                "Bloomfire - Principal Search Engineer (OpenSearch)"
                                            ]
                                        },
                                        "field": {
                                            "custom_field_id": "cf_e97HRUrCmP2j7g0tzmTxXWMFr2qGR0jR7nmRu4KR1qv",
                                            "type": "custom_field"
                                        },
                                        "negate": False,
                                        "type": "field_condition"
                                    }
                                ],
                                "type": "or"
                            },
                            {
                                "negate": False,
                                "related_object_type": "opportunity",
                                "related_query": {
                                    "negate": False,
                                    "queries": [
                                        {
                                            "condition": {
                                                "type": "term",
                                                "values": [
                                                    "Bloomfire - Principal Search Engineer (OpenSearch)"
                                                ]
                                            },
                                            "field": {
                                                "custom_field_id": "cf_e97HRUrCmP2j7g0tzmTxXWMFr2qGR0jR7nmRu4KR1qv",
                                                "type": "custom_field"
                                            },
                                            "negate": False,
                                            "type": "field_condition"
                                        },
                                        {
                                            "condition": {
                                                "object_ids": [
                                                    "user_AYrfFtTHbHWNRcNxtyYUEQH02YeZl0agcZ7JxPnuLz9",
                                                    "user_Ww7Dy598hUnviVpvMMUHRBCt6GELbfwzn4IpoQelyiL"
                                                ],
                                                "reference_type": "user_or_group",
                                                "type": "reference"
                                            },
                                            "field": {
                                                "field_name": "created_by",
                                                "object_type": "opportunity",
                                                "type": "regular_field"
                                            },
                                            "negate": False,
                                            "type": "field_condition"
                                        }
                                    ],
                                    "type": "and"
                                },
                                "this_object_type": "lead",
                                "type": "has_related"
                            },
                            {
                                "negate": False,
                                "related_object_type": "opportunity",
                                "related_query": {
                                    "negate": False,
                                    "queries": [
                                        {
                                            "condition": {
                                                "mode": "beginning_of_words",
                                                "type": "text",
                                                "value": "31881b5d-c468-407e-b446-21222d0ea498"
                                            },
                                            "field": {
                                                "custom_field_id": "cf_cDMde58MrqXZmWC8UtAc64BlMP7b0HPGEhedozsyIhv",
                                                "type": "custom_field"
                                            },
                                            "negate": False,
                                            "type": "field_condition"
                                        },
                                        {
                                            "condition": {
                                                "object_ids": [
                                                    "user_AYrfFtTHbHWNRcNxtyYUEQH02YeZl0agcZ7JxPnuLz9",
                                                    "user_Ww7Dy598hUnviVpvMMUHRBCt6GELbfwzn4IpoQelyiL"
                                                ],
                                                "reference_type": "user_or_group",
                                                "type": "reference"
                                            },
                                            "field": {
                                                "field_name": "created_by",
                                                "object_type": "opportunity",
                                                "type": "regular_field"
                                            },
                                            "negate": False,
                                            "type": "field_condition"
                                        }
                                    ],
                                    "type": "and"
                                },
                                "this_object_type": "lead",
                                "type": "has_related"
                            }
                        ],
                        "type": "or"
                    }
                ],
                "type": "and"
            }
        ],
        "type": "and"
    },
    "sort": []
}


# Load list of searches from external JSON
with open("searches.json", "r") as f:
    SEARCHES = json.load(f)



# =============================================================================
# 🔁 Updated Close CRM Pagination (Cursor Pagination)
# =============================================================================
def run_close_query(json_filter):
    total_results = []
    cursor = None  # cursor must start as None

    while True:
        payload = dict(json_filter)
        payload["_limit"] = 200
        payload["cursor"] = cursor

        response = requests.post(
            CLOSE_API_URL,
            auth=(CLOSE_API_KEY, ""),
            json=payload
        )

        if response.status_code != 200:
            print("\n❌ Close API error:", response.text)
            return None

        data = response.json()

        # collect results
        batch = data.get("data", [])
        total_results.extend(batch)

        # next cursor
        cursor = data.get("cursor")

        # stop when cursor is null
        if cursor is None:
            break

    return len(total_results)



# =============================================================================
# 🚀 MAIN SYNC FUNCTION
# =============================================================================
def main():
    print("\n🔄 Running Multi-Saved-Search CloseCRM → Google Sheets Sync...\n")

    sheet = gc.open_by_key(SHEET_ID).worksheet(SHEET_NAME)

    for search in SEARCHES:
    name = search["name"]
    cell = search["cell"]

    # Load filter JSON file
    filter_path = os.path.join("filters", search["filter_file"])
    with open(filter_path, "r") as f:
        json_filter = json.load(f)

    print(f"📌 Processing search: {name}")

    count = run_close_query(json_filter)

    if count is None:
        print(f"❌ Error while processing {name}")
        continue

    print(f"   ✅ Leads matching filter: {count}")
    print(f"   ✏️ Writing value to {SHEET_NAME} cell {cell}")

    sheet.update_acell(cell, count)


    print("\n🎉 Sync complete.\n")



if __name__ == "__main__":
    main()

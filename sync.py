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
SHEET_ID = os.getenv("SHEET_ID")
TAB_NAME = "Sheet11"
TARGET_CELL = "B2"   # Change if needed

with open(GOOGLE_SA_FILE, "r") as f:
    service_account_info = json.load(f)

credentials = Credentials.from_service_account_info(
    service_account_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)

gc = gspread.authorize(credentials)
sheet = gc.open_by_key(SHEET_ID).worksheet(TAB_NAME)

# -----------------------------
# Your JSON Filter
# -----------------------------
CUSTOM_FILTER = {
    "limit": None,
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
    "results_limit": None,
    "sort": [
        {
            "direction": "desc",
            "field": {
                "field_name": "max_opportunity_annualized_annualized_value",
                "object_type": "lead",
                "type": "regular_field"
            }
        },
        {
            "direction": "asc",
            "field": {
                "field_name": "date_updated",
                "object_type": "lead",
                "type": "regular_field"
            }
        }
    ]
}


# -----------------------------
# Run the filter on CloseCRM
# -----------------------------
def run_close_filter(json_filter):
    response = requests.post(
        CLOSE_API_URL,
        auth=(CLOSE_API_KEY, ""),
        json=json_filter,
        headers={"Content-Type": "application/json"}
    )

    if response.status_code != 200:
        print("❌ CloseCRM error:", response.text)
        return None

    return len(response.json().get("data", []))

# -----------------------------
# MAIN
# -----------------------------
def main():
    print("🚀 Running CloseCRM → Google Sheets Sync")

    count = run_close_filter(CUSTOM_FILTER)

    if count is None:
        print("❌ API error — no update written.")
        return

    print(f"📊 Leads matching filter: {count}")
    print(f"📝 Writing to {TAB_NAME} cell {TARGET_CELL}")

    sheet.update_acell(TARGET_CELL, count)

    print("✅ Sync complete!")

if __name__ == "__main__":
    main()

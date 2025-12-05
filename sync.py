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
# 🔍 CORRECT BLOOMFIRE JSON FILTER (Matches UI Exactly)
# =============================================================================
BLOOMFIRE_FILTER ={
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
    "results_limit": 0,
    "sort": []
}




# =============================================================================
# 📌 LIST OF METRICS TO WRITE INTO GOOGLE SHEETS
# =============================================================================
SEARCHES = [
    {
        "name": "Bloomfire - Principal Search Engineer",
        "cell": "B2",
        "filter": BLOOMFIRE_FILTER
    },
    # Add more saved-search JSONs here:
    # {
    #     "name": "Another Saved Search",
    #     "cell": "B3",
    #     "filter": OTHER_FILTER_JSON
    # }
]


# =============================================================================
# 🔁 Close CRM Query Helper (supports pagination)
# =============================================================================
def run_close_query(json_filter):
    total_results = []
    skip = 0
    page_size = 100

    while True:
        paginated_filter = dict(json_filter)
        paginated_filter["skip"] = skip
        paginated_filter["limit"] = page_size

        response = requests.post(
            CLOSE_API_URL,
            auth=(CLOSE_API_KEY, ""),
            json=paginated_filter
        )

        if response.status_code != 200:
            print("\n❌ Close API error:", response.text)
            return None

        batch = response.json().get("data", [])
        total_results.extend(batch)

        if len(batch) < page_size:
            break

        skip += page_size

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
        json_filter = search["filter"]

        print(f"📌 Processing search: {name}")
        count = run_close_query(json_filter)

        if count is None:
            print(f"   ❌ Skipping {name} due to API error.\n")
            continue

        print(f"   ✅ Leads matching filter: {count}")
        print(f"   ✏️ Writing value to {SHEET_NAME} cell {cell}\n")

        sheet.update_acell(cell, count)

    print("🎉 Sync complete! All saved searches updated.\n")


if __name__ == "__main__":
    main()

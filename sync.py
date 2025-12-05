import os
import json
import requests
import gspread
from google.oauth2.service_account import Credentials


# -----------------------------
# CloseCRM Setup
# -----------------------------

CLOSE_API_KEY = os.getenv("CLOSE_API_KEY")
CLOSE_SAVED_SEARCH_URL = "https://api.close.com/api/v1/saved_search/"
CLOSE_SEARCH_URL = "https://api.close.com/api/v1/data/search/"



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


# -----------------------------
# DEFINE ALL SAVED SEARCHES HERE
# -----------------------------

METRICS = [
    {
        "name": "Bloomfire - Principal Search Engineer",
        "saved_search_id": "save_n75gvVtflCk7eBpqirqQzvc2hFSBzJNZSlWyXXs0djY",
        "tab": "Sheet11",
        "cell": "B2"
    },
    # Add more saved searches here:
    # {
    #     "name": "Another Search",
    #     "saved_search_id": "save_ABC123XYZ",
    #     "tab": "Sheet11",
    #     "cell": "B3"
    # },
]



# -----------------------------
# HELPERS
# -----------------------------

def fetch_saved_search_query(saved_search_id):
    """Pull the REAL internal query Close uses in the UI."""
    url = f"{CLOSE_SAVED_SEARCH_URL}{saved_search_id}/"
    resp = requests.get(url, auth=(CLOSE_API_KEY, ""))

    if resp.status_code != 200:
        print(f"❌ Error loading saved search {saved_search_id}: {resp.text}")
        return None

    return resp.json().get("query")


def count_matching_leads(query):
    """Run the Close search with pagination to count ALL leads."""
    
    payload = {
        "query": query,
        "type": "lead",
        "limit": 100,         # Small page size = safe for pagination
        "results_limit": 5000
    }

    total = 0
    has_more = True

    while has_more:
        resp = requests.post(
            CLOSE_SEARCH_URL,
            auth=(CLOSE_API_KEY, ""),
            json=payload,
            headers={"Content-Type": "application/json"}
        )

        if resp.status_code != 200:
            print("❌ CloseCRM API error:", resp.text)
            return None

        data = resp.json()
        batch = len(data.get("data", []))
        total += batch

        cursor = data.get("cursor")
        if cursor:
            payload["cursor"] = cursor
        else:
            has_more = False

    return total



# -----------------------------
# MAIN SYNC PROCESS
# -----------------------------

def main():
    print("🚀 Running Multi-Saved-Search CloseCRM → Google Sheets Sync...\n")

    for metric in METRICS:
        name = metric["name"]
        saved_search_id = metric["saved_search_id"]
        tab = metric["tab"]
        cell = metric["cell"]

        print(f"🔎 Processing: {name}")
        print(f"   Saved Search ID: {saved_search_id}")

        # Load real internal Close query
        query = fetch_saved_search_query(saved_search_id)
        if query is None:
            print(f"❌ Skipping {name} due to query fetch error.\n")
            continue

        # Run search and count all matching leads
        count = count_matching_leads(query)
        if count is None:
            print(f"❌ Skipping {name} due to API error.\n")
            continue

        print(f"📊 Leads found: {count}")

        # Update Google Sheets
        sheet = gc.open_by_key(os.getenv("SHEET_ID")).worksheet(tab)
        sheet.update_acell(cell, count)
        print(f"📝 Updated {tab} cell {cell} with {count}\n")

    print("✅ Sync complete! All saved searches updated.\n")



if __name__ == "__main__":
    main()

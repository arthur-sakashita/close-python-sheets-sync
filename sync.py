import os
import json
import requests
import gspread
from google.oauth2.service_account import Credentials

# ------------------------
#  CloseCRM Setup
# ------------------------
CLOSE_API_KEY = os.getenv("CLOSE_API_KEY")
SMARTVIEW_ID = "your-smartview-id-here"  # you can hardcode or make a secret

CLOSE_API_URL = f"https://api.close.com/api/v1/saved_search/{SMARTVIEW_ID}/execute/"

# ------------------------
#  Google Sheets Setup
# ------------------------
service_account_info = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
credentials = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)

gc = gspread.authorize(credentials)
sheet = gc.open_by_key(os.getenv("SHEET_ID")).sheet1

# ------------------------
#  Fetch data from Close
# ------------------------
headers = {"Content-Type": "application/json"}
auth = (CLOSE_API_KEY, "")  # API key as username, empty password

response = requests.get(CLOSE_API_URL, auth=auth)

if response.status_code != 200:
    print("Error fetching Close data:", response.text)
    exit(1)

close_data = response.json()

# Extract results (depends how your SmartView is structured)
rows = close_data.get("data", [])

# ------------------------
#  Write into Google Sheets
# ------------------------

# Clear sheet
sheet.clear()

# Write header row
if rows:
    header = list(rows[0].keys())
    sheet.append_row(header)

    for row in rows:
        sheet.append_row(list(row.values()))

print("✅ Sync complete!")

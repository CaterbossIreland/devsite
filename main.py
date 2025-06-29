from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from io import BytesIO
import pandas as pd
import requests
import os

app = FastAPI()

# === CORS ===
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# === Graph API Config ===
TENANT_ID = "ce280aae-ee92-41fe-ab60-66b37ebc97dd"
CLIENT_ID = "83acd574-ab02-4cfe-b28c-e38c733d9a52"
CLIENT_SECRET = "FYX8Q~bZVXuKEnMTryxYw-ZuQqO20BTNU8Qa~1"

SITE_ID = "caterboss.sharepoint.com,798d8a1b-c8b4-493e-b320-be94a4c165a1,ec07bde5-4a37-459a-92ef-a58100f17191"
DRIVE_ID = "b!udRZ7OsrmU61CSAYEn--q1fPtuPR3TZAsv2B9cCW-gzWb8B-lsUaQLURaNYNJxjP"

STOCK_FILE_IDS = [
    "01YTGSV5HJCNBDXINJP5FJE2TICQ6Q3NEX",  # Nisbets
    "01YTGSV5FBVS7JYODGLREKL273FSJ3XRLP",  # Nortons
]

SUPPLIER_FILE_ID = "01YTGSV5ALH67IM5W73JDJ422J6AOUCC6M"  # Supplier.csv

# === Token Fetching ===
def get_access_token_sync():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": CLIENT_ID,
        "scope": "https://graph.microsoft.com/.default",
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials",
    }
    response = requests.post(url, data=data)
    return response.json()["access_token"]

# === Graph API File Fetch ===
def download_excel_file(item_id: str) -> pd.DataFrame:
    token = get_access_token_sync()
    endpoint = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{item_id}/content"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(endpoint, headers=headers)
    if response.status_code != 200:
        raise HTTPException(status_code=500, detail=f"Download failed for file ID: {item_id}")
    return pd.read_excel(BytesIO(response.content))

def download_csv_file(item_id: str) -> pd.DataFrame:
    token = get_access_token_sync()
    endpoint = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{item_id}/content"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(endpoint, headers=headers)
    if response.status_code != 200:
        raise HTTPException(status_code=500, detail=f"Download failed for CSV ID: {item_id}")
    return pd.read_csv(BytesIO(response.content))

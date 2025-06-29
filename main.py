from fastapi import FastAPI, UploadFile, File, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import requests
import os
from io import BytesIO

from graph_excel import (
    get_excel_file_metadata,
    list_excel_sheets,
    read_sheet_data
)

app = FastAPI()

# Optional CORS for browser/API testing
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# === ENV & AUTH CONFIG ===
TENANT_ID = "ce280aae-ee92-41fe-ab60-66b37ebc97dd"
CLIENT_ID = "83acd574-ab02-4cfe-b28c-e38c733d9a52"
CLIENT_SECRET = "FYX8Q~bZVXuKEenMTryxYw-ZuQOq2OBTNIu8Qa~i"
SITE_ID = "caterboss.sharepoint.com,798d8a1b-c8b4-493e-b320-be94a4c165a1,ec07bde5-4a37-459a-92ef-a58100f17191"
DRIVE_ID = "b!udRZ7OsrmU61CSAYEn--q1fPtuPR3TZAsv2B9cCW-gzWb8B-lsUaQLURaNYNJxjP"
STOCK_FILE_IDS = [
    "01YTGSV5HJCNBDXINJP5FJE2TICQ6Q3NEX",
    "01YTGSV5FBVS7JYODGLREKL273FSJ3XRLP"
]

# === AUTH ===
def get_access_token_sync():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": CLIENT_ID,
        "scope": "https://graph.microsoft.com/.default",
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials"
    }
    response = requests.post(url, data=data)
    if response.status_code != 200:
        raise HTTPException(status_code=500, detail="Failed to obtain token")
    return response.json()["access_token"]

# === ExcelFileRequest for POST APIs ===
class ExcelFileRequest(BaseModel):
    site_id: str
    drive_id: str
    item_id: str

# === ROUTES ===

@app.get("/")
def root():
    return {"message": "Server is up and running."}

@app.get("/get-file-id")
def get_file_id():
    file_id = get_excel_file_metadata()
    return {"file_id": file_id}

@app.get("/sheets")
def get_sheets():
    file_id = "01YTGSV5HJCNBDXINJP5FJE2TICQ6Q3NEX"
    sheets = list_excel_sheets(file_id)
    return {"sheets": sheets}

@app.get("/stock-data")
def get_stock_data():
    file_id = "01YTGSV5HJCNBDXINJP5FJE2TICQ6Q3NEX"
    data = read_sheet_data(file_id)
    return {"rows": data[:10]}  # limit for test

@app.get("/list_sites")
def list_sites():
    token = get_access_token_sync()
    url = "https://graph.microsoft.com/v1.0/sites?search=*"
    headers = {"Authorization": f"Bearer {token}"}
    return requests.get(url, headers=headers).json()

@app.get("/list_drives")
def list_drives():
    token = get_access_token_sync()
    url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives"
    headers = {"Authorization": f"Bearer {token}"}
    return requests.get(url, headers=headers).json()

@app.get("/list_files")
def list_files():
    token = get_access_token_sync()
    url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/root/children"
    headers = {"Authorization": f"Bearer {token}"}
    return requests.get(url, headers=headers).json()

@app.post("/read_excel")
def read_excel(request: ExcelFileRequest):
    token = get_access_token_sync()
    url = f"https://graph.microsoft.com/v1.0/sites/{request.site_id}/drives/{request.drive_id}/items/{request.item_id}/workbook/worksheets"
    headers = {"Authorization": f"Bearer {token}"}
    return requests.get(url, headers=headers).json()

@app.post("/write_excel")
def write_excel(request: ExcelFileRequest):
    token = get_access_token_sync()
    url = f"https://graph.microsoft.com/v1.0/sites/{request.site_id}/drives/{request.drive_id}/items/{request.item_id}/workbook/worksheets('Sheet1')/range(address='A1')"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    data = {"values": [["Updated by FastAPI!"]]}
    response = requests.patch(url, headers=headers, json=data)
    return {"message": "Cell A1 updated"}

@app.post("/process_orders")
async def process_orders(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        df = pd.read_excel(BytesIO(contents))

        COLUMN_ALIASES = {
            "ORDER NO": "ORDER",
            "ORDER NUMBER": "ORDER",
            "PRODUCT CODE": "SKU",
            "ITEM CODE": "SKU",
            "QUANTITY": "QTY",
            "QTY.": "QTY",
            "QTY ORDERED": "QTY",
            "ORDER#": "ORDER"
        }

        REQUIRED_COLUMNS = {"ORDER", "SKU", "QTY"}
        df.columns = [COLUMN_ALIASES.get(c.strip().upper(), c.strip().upper()) for c in df.columns]
        missing = REQUIRED_COLUMNS - set(df.columns)
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing columns: {', '.join(missing)}")
        return {"status": "success", "rows": df.shape[0]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")

from fastapi import FastAPI, UploadFile, File, HTTPException
from pydantic import BaseModel
import pandas as pd
import tempfile
import requests
import os

app = FastAPI()

# === GRAPH CONFIG ===
TENANT_ID = "ce280aae-ee92-41fe-ab60-66b37ebc97dd"
CLIENT_ID = "83acd574-ab02-4cfe-b28c-e38c733d9a52"
CLIENT_SECRET = "FYX8Q~bZVXuKEenMTryxYw-ZuQOq2OBTNIu8Qa~i"

# ✅ NEW SITE_ID from list_sites response
SITE_ID = "caterboss.sharepoint.com,798d8a1b-c8b4-493e-b320-be94a4c165a1,ec07bde5-4a37-459a-92ef-a58100f17191"

# ⛔️ TEMP placeholders: Update these with valid DRIVE_ID + item_ids from /list_drives and /list_files
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

# === Excel File Targeting Model ===
class ExcelFileRequest(BaseModel):
    site_id: str
    drive_id: str
    item_id: str

# === Load Stock Data Helper ===
def load_stock_data():
    token = get_access_token_sync()
    headers = {"Authorization": f"Bearer {token}"}
    full_data = pd.DataFrame()

    for file_id in STOCK_FILE_IDS:
        url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/items/{file_id}/workbook/worksheets('Sheet1')/usedRange(valuesOnly=true)"
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise HTTPException(status_code=500, detail=f"Failed to fetch stock file {file_id}: {response.text}")
        raw = response.json()["values"]
        df = pd.DataFrame(raw[1:], columns=raw[0])
        full_data = pd.concat([full_data, df], ignore_index=True)

    return full_data

# === Match Orders with Stock ===
def check_stock_availability(orders_df, stock_df):
    stock_df = stock_df.rename(columns=str.upper)
    orders_df = orders_df.rename(columns=str.upper)

    stock_df["SKU"] = stock_df["SKU"].astype(str)
    orders_df["SKU"] = orders_df["SKU"].astype(str)

    merged = orders_df.merge(stock_df[["SKU", "QTY"]], how="left", on="SKU")
    merged["QTY"] = merged["QTY"].fillna(0).astype(int)
    merged["FROM_STOCK"] = merged[["QUANTITY", "QTY"]].min(axis=1)
    merged["TO_ORDER"] = merged["QUANTITY"] - merged["FROM_STOCK"]

    return merged[["SKU", "QUANTITY", "QTY", "FROM_STOCK", "TO_ORDER"]]

# === Process Orders ===
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from io import BytesIO

app = FastAPI()

# Optional: add CORS if testing from Swagger UI or browser
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Define required columns and aliases
REQUIRED_COLUMNS = {"ORDER", "SKU", "QTY"}

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

def clean_column_name(col):
    """Normalize and map column name to standard alias."""
    col_cleaned = col.strip().upper()
    return COLUMN_ALIASES.get(col_cleaned, col_cleaned)

@app.post("/process_orders")
async def process_orders(file: UploadFile = File(...)):
    try:
        # Load Excel file into DataFrame
        contents = await file.read()
        df = pd.read_excel(BytesIO(contents))

        # Normalize and alias column names
        df.columns = [clean_column_name(c) for c in df.columns]

        # Check for missing required columns
        headers_set = set(df.columns)
        missing = REQUIRED_COLUMNS - headers_set
        if missing:
            raise HTTPException(
                status_code=400,
                detail=f"Processing failed: 400: Missing columns: {', '.join(missing)}"
            )

        # Sample logic: count valid rows
        row_count = df.shape[0]

        return {"status": "success", "rows": row_count}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")

# === Optional Tools ===
@app.get("/list_sites")
def list_sites():
    token = get_access_token_sync()
    url = "https://graph.microsoft.com/v1.0/sites?search=*"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    return response.json()

@app.get("/list_drives")
def list_drives():
    token = get_access_token_sync()
    url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    return response.json()

@app.get("/list_files")
def list_files():
    token = get_access_token_sync()
    url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/root/children"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    return response.json()

@app.post("/read_excel")
def read_excel(request: ExcelFileRequest):
    token = get_access_token_sync()
    url = (
        f"https://graph.microsoft.com/v1.0/sites/{request.site_id}/drives/"
        f"{request.drive_id}/items/{request.item_id}/workbook/worksheets"
    )
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    return response.json()

@app.post("/write_excel")
def write_excel(request: ExcelFileRequest):
    token = get_access_token_sync()
    url = (
        f"https://graph.microsoft.com/v1.0/sites/{request.site_id}/drives/"
        f"{request.drive_id}/items/{request.item_id}/workbook/worksheets('Sheet1')/range(address='A1')"
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    data = {"values": [["Updated by FastAPI!"]]}
    response = requests.patch(url, headers=headers, json=data)
    return {"message": "Cell A1 updated"}
from fastapi import FastAPI
from graph_excel import get_excel_file_metadata

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Server is up and running."}

@app.get("/get-file-id")
def get_file_id():
    file_id = get_excel_file_metadata()
    return {"file_id": file_id}
from graph_excel import list_excel_sheets

@app.get("/sheets")
def get_sheets():
    file_id = "01YTGSV5HJCNBDXINJP5FJE2TICQ6Q3NEX"
    sheets = list_excel_sheets(file_id)
    return {"sheets": sheets}

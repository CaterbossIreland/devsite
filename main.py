from fastapi import FastAPI, UploadFile, File, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import requests
from io import BytesIO

from graph_excel import download_excel_file  # ✅ Just import what exists


app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# === ENV CONFIG ===
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

class ExcelFileRequest(BaseModel):
    site_id: str
    drive_id: str
    item_id: str

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
    return {"rows": data[:10]}

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
            "ORDER#": "ORDER",
            "PRODUCT CODE": "SKU",
            "ITEM CODE": "SKU",
            "OFFER SKU": "SKU",
            "QUANTITY": "QTY",
            "QTY.": "QTY",
            "QTY ORDERED": "QTY"
        }

        REQUIRED_COLUMNS = {"ORDER", "SKU", "QTY"}
        df.columns = [COLUMN_ALIASES.get(c.strip().upper(), c.strip()) for c in df.columns]
        df.columns = [c.upper() for c in df.columns]
        missing = REQUIRED_COLUMNS - set(df.columns)
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing columns: {', '.join(missing)}")

        return {"status": "success", "rows": df.shape[0]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")

@app.post("/check_stock")
async def check_stock(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        df = pd.read_excel(BytesIO(contents))

        COLUMN_ALIASES = {
            "ORDER NO": "ORDER",
            "ORDER NUMBER": "ORDER",
            "ORDER#": "ORDER",
            "PRODUCT CODE": "SKU",
            "ITEM CODE": "SKU",
            "OFFER SKU": "SKU",
            "QUANTITY": "QTY",
            "QTY.": "QTY",
            "QTY ORDERED": "QTY"
        }

        REQUIRED_COLUMNS = {"ORDER", "SKU", "QTY"}
        df.columns = [COLUMN_ALIASES.get(c.strip().upper(), c.strip()) for c in df.columns]
        df.columns = [c.upper() for c in df.columns]
        missing = REQUIRED_COLUMNS - set(df.columns)
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing columns: {', '.join(missing)}")

        stock_data = read_sheet_data("01YTGSV5HJCNBDXINJP5FJE2TICQ6Q3NEX")
        stock_df = pd.DataFrame(stock_data)

        order_df = df[["SKU", "QTY"]].copy()
        order_df["SKU"] = order_df["SKU"].astype(str)
        stock_df["SKU"] = stock_df["SKU"].astype(str)
        stock_df["QTY"] = pd.to_numeric(stock_df["QTY"], errors="coerce").fillna(0).astype(int)

        merged = order_df.groupby("SKU").sum().reset_index()
        merged = merged.merge(stock_df, how="left", on="SKU", suffixes=("_ordered", "_stock"))
        merged["QTY_stock"] = merged["QTY_stock"].fillna(0).astype(int)

        merged["FULFILLED"] = merged[["QTY_ordered", "QTY_stock"]].min(axis=1)
        merged["TO_ORDER"] = merged["QTY_ordered"] - merged["FULFILLED"]

        fulfilled = []
        to_order = []

        for _, row in merged.iterrows():
            if row["FULFILLED"] > 0:
                fulfilled.append({
                    "SKU": row["SKU"],
                    "ordered": int(row["QTY_ordered"]),
                    "stock_before": int(row["QTY_stock"]),
                    "fulfilled": int(row["FULFILLED"]),
                    "stock_after": int(row["QTY_stock"] - row["FULFILLED"])
                })
            if row["TO_ORDER"] > 0:
                to_order.append({
                    "SKU": row["SKU"],
                    "ordered": int(row["QTY_ordered"]),
                    "fulfilled": int(row["FULFILLED"])
                })

        return {
            "fulfilled_from_stock": fulfilled,
            "to_order_from_supplier": to_order
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stock check failed: {str(e)}")
# Insert these just before each OneDrive upload block in your existing main.py
import logging

# Configure logging (prints to Render logs)
logging.basicConfig(level=logging.INFO)

# After creating nisbets_order_df
if to_order_nisbets:
    logging.info("Preparing to upload Nisbets order CSV...")
    logging.info("Nisbets order DataFrame:\n%s", nisbets_order_df)

# After creating nortons_order_df
if to_order_nortons:
    logging.info("Preparing to upload Nortons order CSV...")
    logging.info("Nortons order DataFrame:\n%s", nortons_order_df)

# Right before uploading updated stock Excel for Nisbets
logging.info("Uploading updated Nisbets stock file...\nCurrent qty for J242: %s", nisbets_df[nisbets_df['SKU'] == 'J242'])

# Right before uploading updated stock Excel for Nortons
logging.info("Uploading updated Nortons stock file...\nCurrent qty for J242: %s", nortons_df[nortons_df['SKU'] == 'J242'])

# Also optionally after each upload request:
logging.info("Upload response status for Nisbets CSV: %s", resp.status_code)
logging.info("Upload response body: %s", resp.text)

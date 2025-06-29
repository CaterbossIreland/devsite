from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from io import BytesIO
import pandas as pd
import requests
import os

app = FastAPI()

# Enable CORS for Swagger testing or browser use
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# === Graph Auth Details ===
TENANT_ID = "ce280aae-ee92-41fe-ab60-66b37ebc97dd"
CLIENT_ID = "83acd574-ab02-4cfe-b28c-e38c733d9a52"
CLIENT_SECRET = "FYX8Q~bZVXuKEenMTryxYw-ZuQOq2OBTNIu8Qa~i"

SITE_ID = "caterboss.sharepoint.com,798d8a1b-c8b4-493e-b320-be94a4c165a1,ec07bde5-4a37-459a-92ef-a58100f17191"
DRIVE_ID = "b!udRZ7OsrmU61CSAYEn--q1fPtuPR3TZAsv2B9cCW-gzWb8B-lsUaQLURaNYNJxjP"

STOCK_FILE_IDS = [
    "01YTGSV5HJCNBDXINJP5FJE2TICQ6Q3NEX",
    "01YTGSV5FBVS7JYODGLREKL273FSJ3XRLP"
]

SUPPLIER_PATH = "/Supplier.csv"

# === Auth token generator ===
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

# === Helper: Normalize column names ===
REQUIRED_COLUMNS = {"ORDER", "SKU", "QTY"}
COLUMN_ALIASES = {
    "ORDER NO": "ORDER", "ORDER NUMBER": "ORDER", "ORDER#": "ORDER",
    "PRODUCT CODE": "SKU", "ITEM CODE": "SKU",
    "QUANTITY": "QTY", "QTY.": "QTY", "QTY ORDERED": "QTY"
}
def clean_column_name(col):
    return COLUMN_ALIASES.get(col.strip().upper(), col.strip().upper())

# === Load Stock Excel Files ===
def load_stock_data():
    token = get_access_token_sync()
    headers = {"Authorization": f"Bearer {token}"}
    full_data = pd.DataFrame()

    for file_id in STOCK_FILE_IDS:
        url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/items/{file_id}/workbook/worksheets('Sheet1')/usedRange(valuesOnly=true)"
        resp = requests.get(url, headers=headers)
        if resp.status_code != 200:
            raise HTTPException(status_code=500, detail=f"Failed to fetch stock file {file_id}: {resp.text}")
        values = resp.json()["values"]
        df = pd.DataFrame(values[1:], columns=values[0])
        full_data = pd.concat([full_data, df], ignore_index=True)

    return full_data

# === Download Supplier CSV from OneDrive ===
def download_supplier_csv():
    token = get_access_token_sync()
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/root:{SUPPLIER_PATH}:/content"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        raise HTTPException(status_code=500, detail=f"500: Failed to download supplier.csv: {resp.text}")
    return pd.read_csv(BytesIO(resp.content))

# === Match Orders to Stock ===
def check_stock_availability(orders_df, stock_df):
    stock_df = stock_df.rename(columns=str.upper)
    orders_df = orders_df.rename(columns=str.upper)
    stock_df["SKU"] = stock_df["SKU"].astype(str)
    orders_df["SKU"] = orders_df["SKU"].astype(str)

    merged = orders_df.merge(stock_df[["SKU", "QTY"]], how="left", on="SKU")
    merged["QTY"] = merged["QTY"].fillna(0).astype(int)
    merged["FROM_STOCK"] = merged[["QUANTITY", "QTY"]].min(axis=1)
    merged["TO_ORDER"] = merged["QUANTITY"] - merged["FROM_STOCK"]

    return merged[["ORDER", "SKU", "QUANTITY", "QTY", "FROM_STOCK", "TO_ORDER"]]

# === Group by Supplier ===
def group_by_supplier(df_with_to_order):
    supplier_df = download_supplier_csv()
    supplier_df["SKU"] = supplier_df["SKU"].astype(str)
    df_with_to_order["SKU"] = df_with_to_order["SKU"].astype(str)

    merged = df_with_to_order.merge(supplier_df, on="SKU", how="left")
    grouped = merged.groupby("SUPPLIER").apply(lambda g: g[["ORDER", "SKU", "TO_ORDER"]].to_dict(orient="records")).to_dict()
    return grouped

# === Upload Order and Process ===
@app.post("/process_orders")
async def process_orders(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        df = pd.read_excel(BytesIO(contents))
        df.columns = [clean_column_name(c) for c in df.columns]
        if not REQUIRED_COLUMNS.issubset(df.columns):
            raise HTTPException(status_code=400, detail=f"Missing columns: {REQUIRED_COLUMNS - set(df.columns)}")
        return {"status": "success", "rows": df.shape[0]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")

# === Generate Supplier CSV Output ===
@app.post("/generate-supplier-orders/")
async def generate_supplier_orders(file: UploadFile = File(...)):
    try:
        content = await file.read()
        orders_df = pd.read_excel(BytesIO(content))
        orders_df.columns = [clean_column_name(c) for c in orders_df.columns]
        for col in REQUIRED_COLUMNS:
            if col not in orders_df.columns:
                raise HTTPException(status_code=400, detail=f"Missing column: {col}")

        stock_df = load_stock_data()
        matched_df = check_stock_availability(orders_df, stock_df)
        grouped = group_by_supplier(matched_df)

        return {"status": "success", "supplier_orders": grouped}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Generation failed: {str(e)}")

# === Developer Utilities ===
class ExcelFileRequest(BaseModel):
    site_id: str
    drive_id: str
    item_id: str

@app.get("/list_sites")
def list_sites():
    token = get_access_token_sync()
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get("https://graph.microsoft.com/v1.0/sites?search=*", headers=headers)
    return resp.json()

@app.get("/list_drives")
def list_drives():
    token = get_access_token_sync()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives"
    return requests.get(url, headers=headers).json()

@app.get("/list_files")
def list_files():
    token = get_access_token_sync()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/root/children"
    return requests.get(url, headers=headers).json()

@app.post("/read_excel")
def read_excel(req: ExcelFileRequest):
    token = get_access_token_sync()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/sites/{req.site_id}/drives/{req.drive_id}/items/{req.item_id}/workbook/worksheets"
    return requests.get(url, headers=headers).json()

@app.post("/write_excel")
def write_excel(req: ExcelFileRequest):
    token = get_access_token_sync()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    url = f"https://graph.microsoft.com/v1.0/sites/{req.site_id}/drives/{req.drive_id}/items/{req.item_id}/workbook/worksheets('Sheet1')/range(address='A1')"
    data = {"values": [["Updated by FastAPI!"]]}
    return requests.patch(url, headers=headers, json=data).json()

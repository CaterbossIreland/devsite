# === main.py ===
from fastapi import FastAPI, UploadFile, File, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import requests
from io import BytesIO
from datetime import datetime
import logging

from graph_files import (
    download_excel_file,
    download_csv_file,
    update_excel_file,
    upload_csv_file,
    upload_stock_update
)

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
DRIVE_ID = "b!udRZ7OsrmU61CSAYEn--q1fPtuPR3TZAs"
NISBETS_STOCK_FILE_ID = "01YTGSV5HJCNBDXINJP5FJE2TICQ6Q3NEX"
NORTONS_STOCK_FILE_ID = "01YTGSV5FBVS7JYODGLREKL273FSJ3XRLP"
SUPPLIER_FILE_ID = "01YTGSV5DAKWLM2J6U6ZB2JSTGT7V3MQMV"

@app.post("/generate-docs/")
async def generate_supplier_docs(file: UploadFile = File(...)):
    try:
        order_df = pd.read_excel(BytesIO(await file.read()))
        order_df.columns = [c.upper().strip() for c in order_df.columns]

        sku_column = next((col for col in order_df.columns if col in ["SKU", "PRODUCT CODE", "ITEM CODE", "OFFER SKU"]), None)
        qty_column = next((col for col in order_df.columns if col in ["QTY", "QUANTITY", "QTY.", "QTY ORDERED"]), None)
        order_column = next((col for col in order_df.columns if col in ["ORDER", "ORDER NO", "ORDER NUMBER", "ORDER#"]), None)

        if not all([sku_column, qty_column, order_column]):
            raise HTTPException(status_code=400, detail="Missing expected column(s) in uploaded order sheet.")

        supplier_df = download_excel_file(DRIVE_ID, SUPPLIER_FILE_ID)
        supplier_map = {
            row["SKU"].strip(): row["SUPPLIER"].strip().lower()
            for _, row in supplier_df.iterrows()
            if pd.notna(row.get("SKU")) and pd.notna(row.get("SUPPLIER"))
        }

        nortons, nisbets = [], []
        stock_nortons = download_excel_file(DRIVE_ID, NORTONS_STOCK_FILE_ID)
        stock_nisbets = download_excel_file(DRIVE_ID, NISBETS_STOCK_FILE_ID)
        stock_nortons_dict = dict(zip(stock_nortons['SKU'], stock_nortons['QTY']))
        stock_nisbets_dict = dict(zip(stock_nisbets['SKU'], stock_nisbets['QTY']))
        stock_decrement = {"nortons": {}, "nisbets": {}}

        for _, row in order_df.iterrows():
            sku = str(row[sku_column]).strip()
            qty = int(row[qty_column])
            order_no = str(row[order_column]).strip()
            supplier = supplier_map.get(sku, "").lower()

            if supplier == "nortons":
                stock_qty = stock_nortons_dict.get(sku, 0)
                if qty > stock_qty:
                    nortons.append((order_no, sku, qty - stock_qty))
                    stock_decrement["nortons"][sku] = stock_decrement["nortons"].get(sku, 0) + (qty - stock_qty)
            elif supplier == "nisbets":
                stock_qty = stock_nisbets_dict.get(sku, 0)
                if qty > stock_qty:
                    nisbets.append((order_no, sku, qty - stock_qty))
                    stock_decrement["nisbets"][sku] = stock_decrement["nisbets"].get(sku, 0) + (qty - stock_qty)

        df_nisbets = pd.DataFrame(nisbets, columns=["ORDER", "SKU", "QTY"])
        filename = f"nisbets_supplier_order_{datetime.now().strftime('%Y-%m-%d_%H%M')}.csv"
        buffer = BytesIO()
        df_nisbets.to_csv(buffer, index=False)
        buffer.seek(0)
        upload_csv_file(DRIVE_ID, filename, buffer.getvalue())

        if stock_decrement["nortons"]:
            updated_nortons = upload_stock_update(stock_nortons, stock_decrement["nortons"])
            update_excel_file(DRIVE_ID, NORTONS_STOCK_FILE_ID, updated_nortons)

        if stock_decrement["nisbets"]:
            updated_nisbets = upload_stock_update(stock_nisbets, stock_decrement["nisbets"])
            update_excel_file(DRIVE_ID, NISBETS_STOCK_FILE_ID, updated_nisbets)

        return {
            "success": True,
            "nisbets_file": filename,
            "nortons_orders": nortons,
            "nisbets_orders": nisbets
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/update-stock/")
async def update_stock(supplier_name: str, items: dict):
    try:
        file_id = NISBETS_STOCK_FILE_ID if supplier_name.lower() == "nisbets" else NORTONS_STOCK_FILE_ID
        stock_df = download_excel_file(DRIVE_ID, file_id)
        updated_df = upload_stock_update(stock_df, items)
        update_excel_file(DRIVE_ID, file_id, updated_df)
        return {"success": True, "updated": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# === graph_auth.py ===
import os
import requests

def get_access_token() -> str:
    token_url = f"https://login.microsoftonline.com/{os.getenv('TENANT_ID')}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": os.getenv("CLIENT_ID"),
        "client_secret": os.getenv("CLIENT_SECRET"),
        "scope": "https://graph.microsoft.com/.default"
    }
    r = requests.post(token_url, data=data)
    if r.status_code != 200:
        raise Exception(f"Token fetch failed: {r.text}")
    return r.json().get("access_token")


# === graph_files.py ===
import requests
import pandas as pd
from io import BytesIO
from graph_auth import get_access_token

def download_excel_file(drive_id: str, item_id: str) -> pd.DataFrame:
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/content"
    headers = {"Authorization": f"Bearer {get_access_token()}"}
    r = requests.get(url, headers=headers)
    return pd.read_excel(BytesIO(r.content))

def update_excel_file(drive_id: str, item_id: str, df: pd.DataFrame) -> None:
    buffer = BytesIO()
    df.to_excel(buffer, index=False, engine="xlsxwriter")
    buffer.seek(0)
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/content"
    headers = {"Authorization": f"Bearer {get_access_token()}"}
    requests.put(url, headers=headers, data=buffer.read())

def upload_csv_file(drive_id: str, path: str, content: bytes) -> str:
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{path}:/content"
    headers = {"Authorization": f"Bearer {get_access_token()}"}
    r = requests.put(url, headers=headers, data=content)
    return r.json().get("id")

def upload_stock_update(stock_df: pd.DataFrame, items: dict) -> pd.DataFrame:
    stock_df = stock_df.copy()
    stock_df['SKU'] = stock_df['SKU'].astype(str)
    stock_df['QTY'] = stock_df['QTY'].fillna(0).astype(int)
    for sku, qty in items.items():
        idx = stock_df[stock_df['SKU'] == sku].index
        if not idx.empty:
            stock_df.loc[idx[0], 'QTY'] -= qty
    return stock_df

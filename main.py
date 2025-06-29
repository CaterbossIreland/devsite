from fastapi import FastAPI, UploadFile, File, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import requests
from io import BytesIO
import logging
from datetime import datetime

from graph_excel import download_excel_file, read_sheet_data, upload_csv_to_onedrive
from graph_files import upload_stock_update
from supplier_logic import identify_supplier
from graph_files import (
    download_excel_file,
    download_csv_file,
    update_excel_file,
    upload_csv_file,
    read_sheet_data,
    upload_stock_update,
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
        # Read uploaded order file
        order_df = pd.read_excel(BytesIO(await file.read()))

        # Standardize column headers
        order_df.columns = [c.upper().strip() for c in order_df.columns]
        sku_column = next((col for col in order_df.columns if col in ["SKU", "PRODUCT CODE", "ITEM CODE", "OFFER SKU"]), None)
        qty_column = next((col for col in order_df.columns if col in ["QTY", "QUANTITY", "QTY.", "QTY ORDERED"]), None)
        order_column = next((col for col in order_df.columns if col in ["ORDER", "ORDER NO", "ORDER NUMBER", "ORDER#"]), None)

        if not all([sku_column, qty_column, order_column]):
            raise HTTPException(status_code=400, detail="Missing expected column(s) in uploaded order sheet.")

        # Get supplier mapping
        supplier_df = download_excel_file(DRIVE_ID, SUPPLIER_FILE_ID)
        supplier_map = {
            row["SKU"].strip(): row["SUPPLIER"].strip().lower()
            for _, row in supplier_df.iterrows()
            if pd.notna(row.get("SKU")) and pd.notna(row.get("SUPPLIER"))
        }

        nortons, nisbets = [], []
        stock_nortons = download_excel_file(DRIVE_ID, NORTONS_STOCK_FILE_ID)
        stock_nisbets = download_excel_file(DRIVE_ID, NISBETS_STOCK_FILE_ID)

        # Prepare stock dicts for lookup
        stock_nortons_dict = dict(zip(stock_nortons['SKU'], stock_nortons['QTY']))
        stock_nisbets_dict = dict(zip(stock_nisbets['SKU'], stock_nisbets['QTY']))

        stock_decrement = {"nortons": {}, "nisbets": {}}

        # Split SKUs per supplier, subtracting from stock
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

        # Convert to DataFrame for nisbets CSV
        df_nisbets = pd.DataFrame(nisbets, columns=["ORDER", "SKU", "QTY"])
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M")
        filename = f"nisbets_supplier_order_{timestamp}.csv"
        buffer = BytesIO()
        df_nisbets.to_csv(buffer, index=False)
        buffer.seek(0)
        upload_csv_to_onedrive(buffer.getvalue(), filename)

        # Update stock files in-place
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
        if supplier_name.lower() == "nisbets":
            stock_file_id = NISBETS_STOCK_FILE_ID
        elif supplier_name.lower() == "nortons":
            stock_file_id = NORTONS_STOCK_FILE_ID
        else:
            raise HTTPException(status_code=400, detail="Unknown supplier name")

        stock_df = download_excel_file(DRIVE_ID, stock_file_id)
        updated_stock_df = upload_stock_update(stock_df, items)
        update_excel_file(DRIVE_ID, stock_file_id, updated_stock_df)
        return {"success": True, "updated": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

from fastapi import FastAPI, UploadFile, File, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import requests
from io import BytesIO
import logging

from graph_excel import download_excel_file, read_sheet_data, upload_stock_update, upload_csv_to_onedrive
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

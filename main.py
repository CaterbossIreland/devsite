from fastapi import FastAPI, UploadFile, File, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import requests
from io import BytesIO
import logging

from graph_excel import download_excel_file, read_sheet_data, upload_stock_update, upload_csv_to_onedrive
from supplier_logic import identify_supplier

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
    "01YTGSV5HJCNBDXINJP5FJE2TICQ6Q3NEX",  # Nortons
    "01YTGSV5FBVS7JYODGLREKL273FSJ3XRLP"   # Nisbets
]
SUPPLIER_FILE_ID = "01YTGSV5CKQF4CMEH5GZC2GFF5VMIWBP3B"

# Logging
logging.basicConfig(level=logging.INFO)

# Auth

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

# Models

class ExcelFileRequest(BaseModel):
    site_id: str
    drive_id: str
    item_id: str

@app.get("/")
def root():
    return {"message": "Server is up and running."}

@app.post("/process_orders")
async def process_orders(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        df = pd.read_excel(BytesIO(contents))

        # Standardize column names
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

        df.columns = [COLUMN_ALIASES.get(c.strip().upper(), c.strip().upper()) for c in df.columns]
        required = {"ORDER", "SKU", "QTY"}
        if not required.issubset(set(df.columns)):
            raise HTTPException(status_code=400, detail="Missing columns")

        supplier_map = read_sheet_data(SUPPLIER_FILE_ID)
        supplier_df = pd.DataFrame(supplier_map)
        supplier_df.columns = [c.upper() for c in supplier_df.columns]
        supplier_df = supplier_df.rename(columns={"SUPPLIER NAME": "SUPPLIER", "SUPPLIER SKU": "SKU"})

        df = df.merge(supplier_df, on="SKU", how="left")

        stock_nortons = pd.DataFrame(read_sheet_data(STOCK_FILE_IDS[0]))
        stock_nisbets = pd.DataFrame(read_sheet_data(STOCK_FILE_IDS[1]))
        stock_nortons.columns = stock_nisbets.columns = ["SKU", "QTY"]

        df_grouped = df.groupby(["SUPPLIER", "SKU"]).agg({"QTY": "sum"}).reset_index()

        to_order_nortons = []
        to_order_nisbets = []
        nisbets_df = stock_nisbets.copy()
        nortons_df = stock_nortons.copy()

        for _, row in df_grouped.iterrows():
            supplier, sku, qty = row["SUPPLIER"], row["SKU"], int(row["QTY"])
            if pd.isna(supplier):
                continue
            target_df = nisbets_df if supplier.lower() == "nisbets" else nortons_df
            match = target_df[target_df["SKU"] == sku]
            stock_qty = int(match["QTY"].values[0]) if not match.empty else 0
            to_fulfill = max(0, qty - stock_qty)
            if to_fulfill > 0:
                (to_order_nisbets if supplier.lower() == "nisbets" else to_order_nortons).append({
                    "SKU": sku, "QTY": to_fulfill
                })
                # Update stock
                if not match.empty:
                    target_df.loc[target_df["SKU"] == sku, "QTY"] = stock_qty - (qty - to_fulfill)
                else:
                    target_df = pd.concat([target_df, pd.DataFrame([[sku, 0]], columns=["SKU", "QTY"])])

        # Prepare files
        if to_order_nisbets:
            nisbets_order_df = pd.DataFrame(to_order_nisbets)
            logging.info("Preparing to upload Nisbets order CSV...")
            logging.info("Nisbets order DataFrame:\n%s", nisbets_order_df)
            upload_csv_to_onedrive(nisbets_order_df, "nisbets_order.csv")

        if to_order_nortons:
            nortons_order_df = pd.DataFrame(to_order_nortons)
            logging.info("Preparing to upload Nortons order CSV...")
            logging.info("Nortons order DataFrame:\n%s", nortons_order_df)
            upload_csv_to_onedrive(nortons_order_df, "nortons_order.csv")

        # Upload stock
        logging.info("Uploading updated Nisbets stock file...\nCurrent qty for J242: %s", nisbets_df[nisbets_df['SKU'] == 'J242'])
        upload_stock_update(nisbets_df, STOCK_FILE_IDS[1])

        logging.info("Uploading updated Nortons stock file...\nCurrent qty for J242: %s", nortons_df[nortons_df['SKU'] == 'J242'])
        upload_stock_update(nortons_df, STOCK_FILE_IDS[0])

        return {
            "nisbets": to_order_nisbets,
            "nortons": to_order_nortons
        }

    except Exception as e:
        logging.exception("Error in /process_orders")
        raise HTTPException(status_code=500, detail=str(e))

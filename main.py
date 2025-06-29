from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
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

# === Upload Result File ===
def upload_to_onedrive(filename: str, df: pd.DataFrame):
    token = get_access_token_sync()
    endpoint = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/root:/Generated/{filename}:/content"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }
    with BytesIO() as output:
        df.to_excel(output, index=False)
        output.seek(0)
        response = requests.put(endpoint, headers=headers, data=output.read())
    if response.status_code >= 300:
        raise HTTPException(status_code=response.status_code, detail="Upload failed")

# === Main Endpoint ===
@app.post("/generate-docs/")
async def generate_docs(file: UploadFile = File(...)):
    try:
        uploaded_orders = pd.read_csv(BytesIO(await file.read()))
        supplier_df = download_csv_file(SUPPLIER_FILE_ID)
        supplier_map = supplier_df.set_index("SKU")["Supplier"].to_dict()

        supplier_orders = {}
        for file_id in STOCK_FILE_IDS:
            stock_df = download_excel_file(file_id)
            stock_skus = set(stock_df["SKU"].astype(str).str.strip().unique())
            uploaded_orders["SKU"] = uploaded_orders["SKU"].astype(str).str.strip()
            needed = uploaded_orders[~uploaded_orders["SKU"].isin(stock_skus)]

            for _, row in needed.iterrows():
                sku = row["SKU"]
                supplier = supplier_map.get(sku, "Unknown")
                if supplier not in supplier_orders:
                    supplier_orders[supplier] = []
                supplier_orders[supplier].append(row)

        for supplier, rows in supplier_orders.items():
            supplier_df = pd.DataFrame(rows)
            filename = f"{supplier}_order_list.xlsx"
            upload_to_onedrive(filename, supplier_df)

        return {"message": "Supplier order files generated and uploaded."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

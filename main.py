from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from io import BytesIO
import pandas as pd
import requests
import os
import zipfile
from tempfile import TemporaryDirectory

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
CLIENT_SECRET = "FYX8Q~bZVXuKEenMTryxYw-ZuQOq2OBTNIu8Qa~i"  # use correct secret here

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
    if response.status_code != 200:
        raise HTTPException(status_code=500, detail="Failed to fetch access token")
    return response.json()["access_token"]

# === Graph API File Fetch ===
def download_excel_file(item_id: str) -> pd.DataFrame:
    token = get_access_token_sync()
    endpoint = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{item_id}/content"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(endpoint, headers=headers)
    if response.status_code != 200:
        raise HTTPException(status_code=500, detail=f"Download failed for file ID: {item_id}")
    return pd.read_excel(BytesIO(response.content), engine="openpyxl")

def download_csv_file(item_id: str) -> pd.DataFrame:
    token = get_access_token_sync()
    endpoint = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{item_id}/content"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(endpoint, headers=headers)
    if response.status_code != 200:
        raise HTTPException(status_code=500, detail=f"Download failed for CSV ID: {item_id}")
    return pd.read_csv(BytesIO(response.content))

# === Main Endpoint ===
@app.post("/generate-docs/")
async def generate_docs(file: UploadFile = File(...)):
    try:
        # Read uploaded Excel file
        contents = await file.read()
        df = pd.read_excel(BytesIO(contents), engine="openpyxl")

        # Normalize and map SKU column
        COLUMN_ALIASES = {
            "PRODUCT CODE": "SKU",
            "ITEM CODE": "SKU",
            "OFFER SKU": "SKU",
            "SKU": "SKU",
        }
        normalized_columns = {col.strip().upper(): col for col in df.columns}
        sku_col = next((normalized_columns.get(alias) for alias in COLUMN_ALIASES if alias in normalized_columns), None)

        if not sku_col:
            raise HTTPException(status_code=400, detail="None of ['SKU'] are in the columns")

        # Rename detected SKU column to 'SKU' for consistency
        df.rename(columns={sku_col: "SKU"}, inplace=True)

        if "SKU" not in df.columns:
            raise HTTPException(status_code=400, detail="SKU column missing after normalization")

        # Download supplier map and create lookup dict
        supplier_df = download_csv_file(SUPPLIER_FILE_ID)
        supplier_map = supplier_df.set_index("SKU")["Supplier"].to_dict()

        # Load stock files and exclude SKUs already in stock
        needed_orders = df.copy()
        for file_id in STOCK_FILE_IDS:
            stock_df = download_excel_file(file_id)
            stock_skus = set(stock_df["SKU"].astype(str).str.strip().unique())
            needed_orders = needed_orders[~needed_orders["SKU"].astype(str).str.strip().isin(stock_skus)]

        # Group needed orders by supplier
        supplier_orders = {}
        for _, row in needed_orders.iterrows():
            sku = str(row["SKU"]).strip()
            supplier = supplier_map.get(sku, "Unknown")
            supplier_orders.setdefault(supplier, []).append(row)

        # Create zip with Excel files for each supplier
        with TemporaryDirectory() as tmpdir:
            zip_path = os.path.join(tmpdir, "supplier_orders.zip")
            with zipfile.ZipFile(zip_path, "w") as zipf:
                for supplier, rows in supplier_orders.items():
                    df_supplier = pd.DataFrame(rows)
                    filename = f"{supplier}_order_list.xlsx"
                    filepath = os.path.join(tmpdir, filename)
                    df_supplier.to_excel(filepath, index=False)
                    zipf.write(filepath, arcname=filename)

            return StreamingResponse(
                open(zip_path, "rb"),
                media_type="application/zip",
                headers={"Content-Disposition": "attachment; filename=supplier_orders.zip"},
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import FileResponse
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
CLIENT_SECRET = "FYX8Q~bZVXuKEnMTryxYw-ZuQqO20BTNU8Qa~1"

DRIVE_ID = "b!udRZ7OsrmU61CSAYEn--q1fPtuPR3TZAsv2B9cCW-gzWb8B-lsUaQLURaNYNJxjP"
SUPPLIER_FILE_ID = "01YTGSV5ALH67IM5W73JDJ422J6AOUCC6M"
STOCK_FILE_IDS = [
    "01YTGSV5HJCNBDXINJP5FJE2TICQ6Q3NEX",  # Nisbets
    "01YTGSV5FBVS7JYODGLREKL273FSJ3XRLP",  # Nortons
]

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
        raise HTTPException(status_code=500, detail=f"Download failed for Excel ID: {item_id}")
    return pd.read_excel(BytesIO(response.content))

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
        contents = await file.read()
        df = pd.read_excel(BytesIO(contents), engine="openpyxl")

        # Normalize column names
        df.columns = [col.strip().upper() for col in df.columns]
        if "OFFER SKU" not in df.columns or "QUANTITY" not in df.columns:
            raise HTTPException(status_code=400, detail="Missing 'Offer SKU' or 'Quantity' column in uploaded file.")

        df["OFFER SKU"] = df["OFFER SKU"].astype(str).str.strip()
        df["QUANTITY"] = pd.to_numeric(df["QUANTITY"], errors="coerce").fillna(0).astype(int)

        # Aggregate total quantity by SKU
        grouped_orders = df.groupby("OFFER SKU", as_index=False)["QUANTITY"].sum()
        grouped_orders = grouped_orders.rename(columns={"OFFER SKU": "SKU"})

        # Load supplier map
        supplier_df = download_csv_file(SUPPLIER_FILE_ID)
        supplier_map = supplier_df.set_index("SKU")["Supplier"].to_dict()

        # Remove SKUs already in stock
        for file_id in STOCK_FILE_IDS:
            stock_df = download_excel_file(file_id)
            stock_df["SKU"] = stock_df["SKU"].astype(str).str.strip()
            in_stock_skus = set(stock_df["SKU"].unique())
            grouped_orders = grouped_orders[~grouped_orders["SKU"].isin(in_stock_skus)]

        # Group orders by supplier
        supplier_orders = {}
        for _, row in grouped_orders.iterrows():
            sku = row["SKU"]
            qty = row["QUANTITY"]
            supplier = supplier_map.get(sku, "Unknown")
            supplier_orders.setdefault(supplier, []).append({"SKU": sku, "Quantity": qty})

        # Create downloadable zip of supplier Excel sheets
        with TemporaryDirectory() as tmpdir:
            zip_path = os.path.join(tmpdir, "supplier_orders.zip")
            with zipfile.ZipFile(zip_path, "w") as zipf:
                for supplier, items in supplier_orders.items():
                    df = pd.DataFrame(items)
                    filepath = os.path.join(tmpdir, f"{supplier}_order_list.xlsx")
                    df.to_excel(filepath, index=False)
                    zipf.write(filepath, arcname=os.path.basename(filepath))

            return FileResponse(
                zip_path,
                filename="supplier_orders.zip",
                media_type="application/zip"
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

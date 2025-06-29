from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import requests
from io import BytesIO

app = FastAPI()

# CORS config
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Microsoft Graph API Credentials
TENANT_ID = "ce280aae-ee92-41fe-ab60-66b37ebc97dd"
CLIENT_ID = "83acd574-ab02-4cfe-b28c-e38c733d9a52"
CLIENT_SECRET = "FYX8Q~bZVXuKEenMTryxYw-ZuQOq2OBTNIu8Qa~i"
DRIVE_ID = "b!_osuAVwo5EyWvEWEMnzopleQal6puNREsmylMfjWpjsv-rD7sQmrQLHDhsQKjaxA"

# Get access token
def get_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default"
    }
    r = requests.post(url, data=data)
    r.raise_for_status()
    return r.json()["access_token"]

# Download CSV from OneDrive
def download_csv(filename: str) -> pd.DataFrame:
    token = get_token()
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/root:/{filename}:/content"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise HTTPException(status_code=500, detail=f"Failed to download {filename}: {response.text}")
    return pd.read_csv(BytesIO(response.content))

# Match SKUs and check stock
def build_supplier_orders(order_df: pd.DataFrame, stock_dfs: list[pd.DataFrame], supplier_map_df: pd.DataFrame) -> pd.DataFrame:
    all_stock = pd.concat(stock_dfs)
    merged = pd.merge(order_df, all_stock, on="SKU", how="left", indicator=True)
    missing_stock = merged[merged["_merge"] == "left_only"]
    result = pd.merge(missing_stock, supplier_map_df, on="SKU", how="left")
    return result[["OrderNumber", "SKU", "Quantity", "Supplier"]]

@app.post("/generate-supplier-orders/")
async def generate_supplier_orders(file: UploadFile = File(...)):
    try:
        order_df = pd.read_csv(file.file)
        nisbets_df = download_csv("Nisbets_Order_List.xlsx")
        nortons_df = download_csv("Nortons_Order_List.xlsx")
        supplier_map_df = download_csv("Supplier.csv")
        result = build_supplier_orders(order_df, [nisbets_df, nortons_df], supplier_map_df)
        return result.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

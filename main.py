from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import requests
from io import BytesIO

# === CONFIG ===
TENANT_ID = "ce280aae-ee92-41fe-ab60-66b37ebc97dd"
CLIENT_ID = "83acd574-ab02-4cfe-b28c-e38c733d9a52"
CLIENT_SECRET = "FYX8Q~bZVXuKEenMTryxYw-ZuQOq2OBTNIu8Qa~i"
DRIVE_ID = "b!udRZ7OsrmU61CSAYEn--q1fPtuPR3TZAs"
NISBETS_STOCK_FILE_ID = "01YTGSV5HJCNBDXINJP5FJE2TICQ6Q3NEX"
NORTONS_STOCK_FILE_ID = "01YTGSV5FBVS7JYODGLREKL273FSJ3XRLP"

# === FASTAPI APP ===
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# === AUTH ===
def get_access_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": "https://graph.microsoft.com/.default",
    }
    resp = requests.post(url, data=data)
    if resp.status_code != 200:
        raise Exception(f"Token fetch failed: {resp.text}")
    return resp.json()["access_token"]

# === GRAPH HELPERS ===
def get_graph_client():
    token = get_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    return requests.Session(), headers

def download_excel_file(drive_id: str, item_id: str) -> pd.DataFrame:
    session, headers = get_graph_client()
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/content"
    resp = session.get(url, headers=headers)
    if resp.status_code != 200:
        raise Exception(f"Failed to download Excel file: {resp.text}")
    return pd.read_excel(BytesIO(resp.content), engine="openpyxl")

def update_excel_file(drive_id: str, item_id: str, df: pd.DataFrame):
    session, headers = get_graph_client()
    buffer = BytesIO()
    df.to_excel(buffer, index=False, engine="openpyxl")
    buffer.seek(0)
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/content"
    resp = session.put(url, headers={"Authorization": headers["Authorization"]}, data=buffer.read())
    if resp.status_code not in (200, 201):
        raise Exception(f"Failed to upload Excel: {resp.text}")

def upload_csv_to_onedrive(drive_id: str, path: str, content: bytes) -> str:
    session, headers = get_graph_client()
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{path}:/content"
    resp = session.put(url, headers={"Authorization": headers["Authorization"]}, data=content)
    if resp.status_code not in (200, 201):
        raise Exception(f"Failed to upload CSV: {resp.text}")
    return resp.json().get("id")

def read_sheet_data(drive_id: str, item_id: str, sheet_name: str) -> list:
    session, headers = get_graph_client()
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/workbook/worksheets('{sheet_name}')/usedRange"
    resp = session.get(url, headers=headers)
    if resp.status_code != 200:
        raise Exception(f"Failed to read sheet range: {resp.text}")
    return resp.json().get("values", [])

def upload_stock_update(stock_df: pd.DataFrame, items: dict) -> pd.DataFrame:
    updated_rows = 0
    for sku, quantity in items.items():
        match = stock_df[stock_df["SKU"].astype(str).str.strip() == str(sku).strip()]
        if not match.empty:
            stock_df.loc[match.index, "QTY"] = quantity
            updated_rows += 1
        else:
            new_row = pd.DataFrame({"SKU": [sku], "QTY": [quantity]})
            stock_df = pd.concat([stock_df, new_row], ignore_index=True)
            updated_rows += 1
    return stock_df

# === API ENDPOINT ===
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

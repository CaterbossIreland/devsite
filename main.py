from fastapi import FastAPI, UploadFile, File, HTTPException
from pydantic import BaseModel
import pandas as pd
import tempfile
import requests
import os

app = FastAPI()

# === GRAPH CONFIG ===
TENANT_ID = "ce280aae-ee92-41fe-ab60-66b37ebc97dd"
CLIENT_ID = "83acd574-ab02-4cfe-b28c-e38c733d9a52"
CLIENT_SECRET = "FYX8Q~bZVXuKEenMTryxYw-ZuQOq2OBTNIu8Qa~i"

SITE_ID = "caterboss.sharepoint.com,7c743e5e-cf99-49a2-8f9c-bc7fa3bc70b1,602a9996-a3a9-473c-9817-3f665aff0fe0"
DRIVE_ID = "b!Xj5dfJnPokmPnLx_o7xwsZaZKmCpozxHmBc_2Ir_D-BcEXAr8106SpXDV8pjRLut"
STOCK_FILE_IDS = [
    "01YRKJEV7QKLNLZDCDFBFLRKFKWQCMKGCW",
    "01YRKJEWCCJQSEAHHTRFDKB2MW2CXJOU3S"
]

# === AUTH ===
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

# === Excel File Targeting Model ===
class ExcelFileRequest(BaseModel):
    site_id: str
    drive_id: str
    item_id: str

# === Load Stock Data Helper ===
def load_stock_data():
    token = get_access_token_sync()
    headers = {"Authorization": f"Bearer {token}"}
    full_data = pd.DataFrame()

    for file_id in STOCK_FILE_IDS:
        url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/items/{file_id}/workbook/worksheets('Sheet1')/usedRange(valuesOnly=true)"
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise HTTPException(status_code=500, detail=f"Failed to fetch stock file {file_id}: {response.text}")
        raw = response.json()["values"]
        df = pd.DataFrame(raw[1:], columns=raw[0])
        full_data = pd.concat([full_data, df], ignore_index=True)

    return full_data

# === Match Orders with Stock ===
def check_stock_availability(orders_df, stock_df):
    stock_df = stock_df.rename(columns=str.upper)
    orders_df = orders_df.rename(columns=str.upper)

    stock_df["SKU"] = stock_df["SKU"].astype(str)
    orders_df["SKU"] = orders_df["SKU"].astype(str)

    merged = orders_df.merge(stock_df[["SKU", "QTY"]], how="left", on="SKU")
    merged["QTY"] = merged["QTY"].fillna(0).astype(int)
    merged["FROM_STOCK"] = merged[["QUANTITY", "QTY"]].min(axis=1)
    merged["TO_ORDER"] = merged["QUANTITY"] - merged["FROM_STOCK"]

    return merged[["SKU", "QUANTITY", "QTY", "FROM_STOCK", "TO_ORDER"]]

# === Process Orders ===
@app.post("/process_orders")
async def process_orders(file: UploadFile = File(...)):
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file.filename)[1]) as tmp:
            contents = await file.read()
            tmp.write(contents)
            tmp_path = tmp.name

        ext = os.path.splitext(file.filename)[-1].lower()
        if ext == ".csv":
            orders_df = pd.read_csv(tmp_path)
        elif ext in [".xls", ".xlsx"]:
            orders_df = pd.read_excel(tmp_path)
        else:
            raise HTTPException(status_code=400, detail="Unsupported file type. Please upload .csv or .xlsx")

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading uploaded file: {e}")

    try:
        stock_df = load_stock_data()
        result_df = check_stock_availability(orders_df, stock_df)

        supplier_list = result_df[result_df["TO_ORDER"] > 0][["SKU", "TO_ORDER"]]
        from_stock_list = result_df[result_df["FROM_STOCK"] > 0][["SKU", "FROM_STOCK"]]

        return {
            "supplier_list": supplier_list.to_dict(orient="records"),
            "dispatch_from_stock": from_stock_list.to_dict(orient="records")
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing failed: {e}")

# === Optional Tools ===
@app.get("/list_sites")
def list_sites():
    token = get_access_token_sync()
    url = "https://graph.microsoft.com/v1.0/sites?search=*"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    return response.json()

@app.get("/list_drives")
def list_drives():
    token = get_access_token_sync()
    url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    return response.json()

@app.get("/list_files")
def list_files():
    token = get_access_token_sync()
    url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/root/children"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    return response.json()

@app.post("/read_excel")
def read_excel(request: ExcelFileRequest):
    token = get_access_token_sync()
    url = (
        f"https://graph.microsoft.com/v1.0/sites/{request.site_id}/drives/"
        f"{request.drive_id}/items/{request.item_id}/workbook/worksheets"
    )
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    return response.json()

@app.post("/write_excel")
def write_excel(request: ExcelFileRequest):
    token = get_access_token_sync()
    url = (
        f"https://graph.microsoft.com/v1.0/sites/{request.site_id}/drives/"
        f"{request.drive_id}/items/{request.item_id}/workbook/worksheets('Sheet1')/range(address='A1')"
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    data = {"values": [["Updated by FastAPI!"]]}
    response = requests.patch(url, headers=headers, json=data)
    return {"message": "Cell A1 updated"}

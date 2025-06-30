import os
import requests
import pandas as pd
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse
from io import BytesIO

# It is much safer to use environment variables for secrets, but for demo, values are hardcoded.
TENANT_ID = os.getenv("TENANT_ID", "ce280aae-ee92-41fe-ab60-66b37ebc97dd")
CLIENT_ID = os.getenv("CLIENT_ID", "83acd574-ab02-4cfe-b28c-e38c733d9a52")
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "FYX8Q~bZVXuKEenMTryxYw-ZuQOq2OBTNIu8Qa~i")
DRIVE_ID = os.getenv("DRIVE_ID", "b!udRZ7OsrmU61CSAYEn--q1fPtuPR3TZAsv2B9cCW-gzWb8B-lsUaQLURaNYNJxjP")
SUPPLIER_FILE_ID = os.getenv("SUPPLIER_FILE_ID", "01YTGSV5DGZEMEISWEYVDJRULO4ADDVCVQ")

def get_graph_access_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default"
    }
    r = requests.post(url, data=data)
    r.raise_for_status()
    return r.json()['access_token']

def download_supplier_csv():
    token = get_graph_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{SUPPLIER_FILE_ID}/content"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    # Read into pandas directly from bytes
    return pd.read_csv(BytesIO(r.content))

app = FastAPI()

@app.post("/upload_orders/")
async def upload_orders(file: UploadFile = File(...)):
    try:
        df = pd.read_excel(file.file)
    except Exception as e:
        return JSONResponse(content={"error": f"Order file read failed: {e}"}, status_code=500)
    try:
        supplier_df = download_supplier_csv()
    except Exception as e:
        return JSONResponse(content={"error": f"Supplier fetch failed: {e}"}, status_code=500)
    try:
        orders = df[['Order number', 'Offer SKU', 'Quantity']].dropna()
    except Exception as e:
        return JSONResponse(content={"error": f"Missing columns in order file: {e}"}, status_code=500)
    try:
        sku_to_supplier = dict(zip(supplier_df['Offer SKU'], supplier_df['Supplier Name']))
        orders['Supplier Name'] = orders['Offer SKU'].map(sku_to_supplier)
    except Exception as e:
        return JSONResponse(content={"error": f"Failed to map SKUs to suppliers: {e}"}, status_code=500)
    try:
        results = {}
        for supplier in ['Nortons', 'Nisbets']:
            supplier_orders = orders[orders['Supplier Name'] == supplier]
            grouped = supplier_orders.groupby('Order number')
            out = []
            for order, group in grouped:
                out.append(f"Order Number: {order}\n")
                for _, row in group.iterrows():
                    out.append(f"·        {int(row['Quantity'])}x {row['Offer SKU']}\n")
                out.append("\n------------------------------\n")
            results[supplier] = "".join(out) if out else "No orders for this supplier."
        return JSONResponse(content=results)
    except Exception as e:
        return JSONResponse(content={"error": f"Failed during output formatting: {e}"}, status_code=500)

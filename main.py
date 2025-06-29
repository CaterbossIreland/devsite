from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from io import BytesIO
import requests

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Hardcoded Graph API Credentials ---
TENANT_ID = "ce280aae-ee92-41fe-ab60-66b37ebc97dd"
CLIENT_ID = "83acd574-ab02-4cfe-b28c-e38c733d9a52"
CLIENT_SECRET = "FYX8Q~bZVXuKEenMTryxYw-ZuQOq2OBTNIu8Qa~i"

# OneDrive Supplier CSV Path
SUPPLIER_FILE_PATH = "/drives/b!udRZ7OsrmU61CSAYEn--q1fPtuPR3TZAs/items/01WJPVUR37I4YJMIOIRZB3VSDJPAJE7N4C/supplier.csv.csv"

def get_graph_token():
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

def download_supplier_csv():
    token = get_graph_token()
    url = f"https://graph.microsoft.com/v1.0/sites/caterboss.sharepoint.com/drive/root:{SUPPLIER_FILE_PATH}:/content"
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return pd.read_csv(BytesIO(r.content))

@app.post("/process")
async def process_order(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        df_orders = pd.read_excel(BytesIO(contents))
        df_orders['SKU'] = df_orders.iloc[:, 13].astype(str)  # Column N

        df_grouped = df_orders.groupby('SKU').size().reset_index(name='QTY')
        df_grouped = df_grouped[df_grouped['SKU'].str.strip().str.lower() != 'nan']

        df_suppliers = download_supplier_csv()

        if not {'SKU', 'SUPPLIER'}.issubset(df_suppliers.columns):
            return JSONResponse(status_code=400, content={"error": "supplier.csv must contain 'SKU' and 'SUPPLIER' columns"})

        merged = pd.merge(df_grouped, df_suppliers, on='SKU', how='left')
        unmatched = merged[merged['SUPPLIER'].isna()]
        matched = merged.dropna(subset=['SUPPLIER'])

        supplier_dict = {}
        for supplier in matched['SUPPLIER'].unique():
            df_supplier = matched[matched['SUPPLIER'] == supplier][['SKU', 'QTY']]
            supplier_dict[supplier] = df_supplier.to_dict(orient='records')

        nisbets_df = matched[matched['SUPPLIER'].str.lower() == "nisbets"]
        nisbets_csv = nisbets_df.to_csv(index=False)

        return {
            "unmatched_skus": unmatched['SKU'].tolist(),
            "suppliers": supplier_dict,
            "nisbets_csv": nisbets_csv
        }

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

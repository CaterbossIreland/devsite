from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import requests
from io import BytesIO

# Import helper functions for Microsoft Graph (adjusted imports)
from graph_auth import get_access_token, get_access_token_sync
from graph_files import download_csv_file, download_excel_file

# === Constants for Microsoft Graph IDs ===
DRIVE_ID = "b!udRZ7OsrmU61CSAYEn--q1fPtuPR3TZAsv2B9cCW-gzWb8B-lsUaQLURaNYNJxjP"
SUPPLIER_FILE_ID = "01YTGSV5ALH67IM5W73JDJ422J6AOUCC6M"
NISBETS_STOCK_FILE_ID = "01YTGSV5HJCNBDXINJP5FJE2TICQ6Q3NEX"
NORTONS_STOCK_FILE_ID = "01YTGSV5FBVS7JYODGLREKL273FSJ3XRLP"

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

@app.post("/generate-docs/")
async def process_order(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        order_df = pd.read_excel(BytesIO(contents), dtype=str)
        required_cols = ['Offer SKU', 'Quantity']
        if not all(col in order_df.columns for col in required_cols):
            raise HTTPException(status_code=400, detail=f"Missing required columns: {required_cols}")
        order_df['Quantity'] = pd.to_numeric(order_df['Quantity'], errors='coerce').fillna(0).astype(int)
        order_df.dropna(subset=['Offer SKU'], inplace=True)
        order_df = order_df[order_df['Quantity'] > 0]
        for col in order_df.columns:
            if str(col).strip().lower() in ['order number', 'order no', 'order#']:
                order_df.rename(columns={col: 'Order'}, inplace=True)
        order_df.rename(columns={'Offer SKU': 'SKU'}, inplace=True)

        supplier_df = download_csv_file(SUPPLIER_FILE_ID)
        expected_sup_cols = ['Supplier Name', 'Supplier SKU']
        if supplier_df.columns[:2].tolist() != expected_sup_cols:
            raise HTTPException(status_code=500, detail="Supplier.csv must have 'Supplier Name' and 'Supplier SKU' as the first two columns")
        supplier_df['Supplier SKU'] = supplier_df['Supplier SKU'].astype(str)
        supplier_map = {str(row['Supplier SKU']).strip(): str(row['Supplier Name']).strip() for _, row in supplier_df.iterrows()}

        nisbets_df = download_excel_file(NISBETS_STOCK_FILE_ID)
        nortons_df = download_excel_file(NORTONS_STOCK_FILE_ID)

        for stock_df, name in [(nisbets_df, 'Nisbets'), (nortons_df, 'Nortons')]:
            if 'SKU' not in stock_df.columns or ('QTY' not in stock_df.columns and 'Quantity' not in stock_df.columns):
                raise HTTPException(status_code=500, detail=f"{name} stock file is missing 'SKU' or 'Quantity' column")
            if 'Quantity' in stock_df.columns:
                stock_df.rename(columns={'Quantity': 'QTY'}, inplace=True)
            stock_df['SKU'] = stock_df['SKU'].astype(str).str.strip()
            stock_df['QTY'] = pd.to_numeric(stock_df['QTY'], errors='coerce').fillna(0).astype(int)
            stock_df.set_index('SKU', inplace=True)

        fulfilled_from_stock = {}
        to_order_nisbets = {}
        to_order_nortons = {}

        for _, row in order_df.iterrows():
            sku = str(row['SKU']).strip()
            qty_needed = int(row['Quantity'])
            order_num = str(row['Order']).strip() if 'Order' in row and pd.notna(row['Order']) else None
            supplier = supplier_map.get(sku)
            if supplier is None or supplier not in ['Nisbets', 'Nortons']:
                raise HTTPException(status_code=400, detail=f"No supplier mapping found for SKU {sku}")
            stock_df = nisbets_df if supplier == 'Nisbets' else nortons_df
            available = int(stock_df.at[sku, 'QTY']) if sku in stock_df.index else 0

            if available >= qty_needed and qty_needed > 0:
                stock_df.at[sku, 'QTY'] = available - qty_needed
                fulfilled_from_stock.setdefault(sku, {'quantity': 0, 'orders': set()})
                fulfilled_from_stock[sku]['quantity'] += qty_needed
                if order_num:
                    fulfilled_from_stock[sku]['orders'].add(order_num)
            elif available > 0 and qty_needed > 0:
                stock_df.at[sku, 'QTY'] = 0
                fulfilled_from_stock.setdefault(sku, {'quantity': 0, 'orders': set()})
                fulfilled_from_stock[sku]['quantity'] += available
                if order_num:
                    fulfilled_from_stock[sku]['orders'].add(order_num)
                remaining = qty_needed - available
                target = to_order_nisbets if supplier == 'Nisbets' else to_order_nortons
                target.setdefault(sku, {'quantity': 0, 'orders': set()})
                target[sku]['quantity'] += remaining
                if order_num:
                    target[sku]['orders'].add(order_num)
            else:
                if qty_needed <= 0:
                    continue
                target = to_order_nisbets if supplier == 'Nisbets' else to_order_nortons
                target.setdefault(sku, {'quantity': 0, 'orders': set()})
                target[sku]['quantity'] += qty_needed
                if order_num:
                    target[sku]['orders'].add(order_num)

        token = get_access_token()
        headers_excel = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        }

        for df, fid, name in [(nisbets_df, NISBETS_STOCK_FILE_ID, 'Nisbets'), (nortons_df, NORTONS_STOCK_FILE_ID, 'Nortons')]:
            df.reset_index(inplace=True)
            with BytesIO() as buf:
                df.to_excel(buf, index=False)
                buf.seek(0)
                resp = requests.put(
                    f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{fid}/content",
                    headers=headers_excel, data=buf.read()
                )
            if resp.status_code >= 300:
                raise HTTPException(status_code=resp.status_code, detail=f"Failed to upload updated {name} stock file")

        def upload_supplier_csv(name, data):
            df = pd.DataFrame([{"SKU": sku, "Quantity": info['quantity']} for sku, info in data.items()])
            csv_data = df.to_csv(index=False)
            resp = requests.put(
                f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/root:/Generated/{name}_Order.csv:/content",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "text/csv"},
                data=csv_data.encode("utf-8")
            )
            if resp.status_code >= 300:
                raise HTTPException(status_code=resp.status_code, detail=f"Failed to upload {name} order CSV")

        if to_order_nisbets:
            upload_supplier_csv("Nisbets", to_order_nisbets)
        if to_order_nortons:
            upload_supplier_csv("Nortons", to_order_nortons)

        def format_allocation(data_dict):
            return [
                {"SKU": sku, "quantity": info['quantity'], "order_numbers": sorted(info['orders'])}
                for sku, info in data_dict.items()
            ]

        return {
            "fulfilled_from_stock": format_allocation(fulfilled_from_stock),
            "to_nisbets": format_allocation(to_order_nisbets),
            "to_nortons": format_allocation(to_order_nortons)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# === Local definition of upload_to_onedrive ===
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
        raise HTTPException(status_code=response.status_code, detail=f"Upload failed: {response.text}")

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import requests
from io import BytesIO

# Import helper functions for Microsoft Graph (assuming these modules are available)
from graph_auth import get_access_token
from graph_files import download_csv_file, download_excel_file, upload_to_onedrive

# === Constants for Microsoft Graph IDs ===
# These should be set to your actual IDs for the target OneDrive and files
DRIVE_ID = "b!udRZ7OsrmU61CSAYEn--q1fPtuPR3TZAsv2B9cCW-gzWb8B-lsUaQLURaNYNJxjP"
SUPPLIER_FILE_ID = "01YTGSV5ALH67IM5W73JDJ422J6AOUCC6M"   # ID of Supplier.csv on OneDrive
NISBETS_STOCK_FILE_ID = "01YTGSV5HJCNBDXINJP5FJE2TICQ6Q3NEX"  # ID of Nisbets stock Excel file
NORTONS_STOCK_FILE_ID = "01YTGSV5FBVS7JYODGLREKL273FSJ3XRLP"  # ID of Nortons stock Excel file

# Initialize FastAPI and configure CORS for broad access
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
    """Process an uploaded Musgraves order Excel file and allocate stock/order accordingly."""
    try:
        # 1. Read the uploaded Excel file (Musgraves order) into a DataFrame
        contents = await file.read()
        order_df = pd.read_excel(BytesIO(contents), dtype=str)
        # Validate required columns in the order file
        required_cols = ['Offer SKU', 'Quantity']
        if not all(col in order_df.columns for col in required_cols):
            raise HTTPException(status_code=400, detail=f"Missing required columns: {required_cols}")
        # Clean and normalize order data
        order_df['Quantity'] = pd.to_numeric(order_df['Quantity'], errors='coerce').fillna(0).astype(int)
        order_df.dropna(subset=['Offer SKU'], inplace=True)  # drop rows with no SKU
        order_df = order_df[order_df['Quantity'] > 0]        # keep only positive quantities
        # If there is an "Order Number" column (or similar), rename it for consistency
        for col in order_df.columns:
            if str(col).strip().lower() in ['order number', 'order no', 'order#']:
                order_df.rename(columns={col: 'Order'}, inplace=True)
        # Rename SKU column for consistency
        order_df.rename(columns={'Offer SKU': 'SKU'}, inplace=True)

        # 2. Download and process Supplier.csv from OneDrive
        supplier_df = download_csv_file(SUPPLIER_FILE_ID)
        # Validate Supplier.csv headers (expect "Supplier Name" and "Supplier SKU")
        expected_sup_cols = ['Supplier Name', 'Supplier SKU']
        if supplier_df.columns[:2].tolist() != expected_sup_cols:
            raise HTTPException(status_code=500, detail="Supplier.csv must have 'Supplier Name' and 'Supplier SKU' as the first two columns")
        # Build a mapping of SKU -> Supplier Name
        supplier_df['Supplier SKU'] = supplier_df['Supplier SKU'].astype(str)
        supplier_map = {str(row['Supplier SKU']).strip(): str(row['Supplier Name']).strip() for _, row in supplier_df.iterrows()}

        # 3. Download current stock files for Nisbets and Nortons from OneDrive
        nisbets_df = download_excel_file(NISBETS_STOCK_FILE_ID)
        nortons_df = download_excel_file(NORTONS_STOCK_FILE_ID)
        # Ensure stock files have 'SKU' and 'QTY' (or 'Quantity') columns
        for stock_df, name in [(nisbets_df, 'Nisbets'), (nortons_df, 'Nortons')]:
            if 'SKU' not in stock_df.columns or ('QTY' not in stock_df.columns and 'Quantity' not in stock_df.columns):
                raise HTTPException(status_code=500, detail=f"{name} stock file is missing 'SKU' or 'Quantity' column")
            # If quantity column is named 'Quantity', rename it to 'QTY'
            if 'Quantity' in stock_df.columns:
                stock_df.rename(columns={'Quantity': 'QTY'}, inplace=True)
            # Normalize types and set SKU as index for easy lookup/update
            stock_df['SKU'] = stock_df['SKU'].astype(str).str.strip()
            stock_df['QTY'] = pd.to_numeric(stock_df['QTY'], errors='coerce').fillna(0).astype(int)
            stock_df.set_index('SKU', inplace=True)

        # 4. Check each SKU in the order against stock and allocate fulfillment
        fulfilled_from_stock = {}   # {SKU: {'quantity': total_fulfilled, 'orders': set(order_numbers)}}
        to_order_nisbets = {}       # {SKU: {'quantity': total_to_order, 'orders': set(order_numbers)}}
        to_order_nortons = {}       # {SKU: {'quantity': total_to_order, 'orders': set(order_numbers)}}

        for _, row in order_df.iterrows():
            sku = str(row['SKU']).strip()
            qty_needed = int(row['Quantity'])
            order_num = str(row['Order']).strip() if 'Order' in row and pd.notna(row['Order']) else None
            # Determine which supplier this SKU should be ordered from if not fulfilled by stock
            supplier = supplier_map.get(sku)
            if supplier is None or supplier not in ['Nisbets', 'Nortons']:
                # Unknown supplier for this SKU
                raise HTTPException(status_code=400, detail=f"No supplier mapping found for SKU {sku}")
            # Select the corresponding stock DataFrame based on supplier
            stock_df = nisbets_df if supplier == 'Nisbets' else nortons_df
            # Current stock available for this SKU
            available = int(stock_df.at[sku, 'QTY']) if sku in stock_df.index else 0

            if available >= qty_needed and qty_needed > 0:
                # Fully fulfill this SKU from stock
                stock_df.at[sku, 'QTY'] = available - qty_needed  # reduce stock quantity
                fulfilled_from_stock.setdefault(sku, {'quantity': 0, 'orders': set()})
                fulfilled_from_stock[sku]['quantity'] += qty_needed
                if order_num:
                    fulfilled_from_stock[sku]['orders'].add(order_num)
            elif available > 0 and qty_needed > 0:
                # Partially fulfill from stock, remainder will be ordered from supplier
                stock_df.at[sku, 'QTY'] = 0  # use all available stock for this SKU
                fulfilled_from_stock.setdefault(sku, {'quantity': 0, 'orders': set()})
                fulfilled_from_stock[sku]['quantity'] += available
                if order_num:
                    fulfilled_from_stock[sku]['orders'].add(order_num)
                # Calculate remaining quantity to order from supplier
                remaining = qty_needed - available
                target_order_list = to_order_nisbets if supplier == 'Nisbets' else to_order_nortons
                target_order_list.setdefault(sku, {'quantity': 0, 'orders': set()})
                target_order_list[sku]['quantity'] += remaining
                if order_num:
                    target_order_list[sku]['orders'].add(order_num)
            else:
                # No stock available at all, route the entire quantity to supplier
                if qty_needed <= 0:
                    continue  # skip if quantity is zero or invalid
                target_order_list = to_order_nisbets if supplier == 'Nisbets' else to_order_nortons
                target_order_list.setdefault(sku, {'quantity': 0, 'orders': set()})
                target_order_list[sku]['quantity'] += qty_needed
                if order_num:
                    target_order_list[sku]['orders'].add(order_num)

        # 5. Upload the updated stock files back to OneDrive (overwriting the originals)
        token = get_access_token()
        headers_excel = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        }
        # Update Nisbets stock file
        nisbets_df.reset_index(inplace=True)  # bring 'SKU' back as a column
        with BytesIO() as buf:
            nisbets_df.to_excel(buf, index=False)
            buf.seek(0)
            resp = requests.put(
                f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{NISBETS_STOCK_FILE_ID}/content",
                headers=headers_excel, data=buf.read()
            )
        if resp.status_code >= 300:
            raise HTTPException(status_code=resp.status_code, detail="Failed to upload updated Nisbets stock file")
        # Update Nortons stock file
        nortons_df.reset_index(inplace=True)
        with BytesIO() as buf:
            nortons_df.to_excel(buf, index=False)
            buf.seek(0)
            resp = requests.put(
                f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{NORTONS_STOCK_FILE_ID}/content",
                headers=headers_excel, data=buf.read()
            )
        if resp.status_code >= 300:
            raise HTTPException(status_code=resp.status_code, detail="Failed to upload updated Nortons stock file")

        # 6. Generate CSV order files for suppliers and upload to OneDrive (in "Generated" folder)
        headers_csv = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "text/csv"
        }
        # Nisbets order CSV (to attach to email, if any items need ordering)
        if to_order_nisbets:
            nisbets_order_df = pd.DataFrame([{"SKU": sku, "Quantity": info['quantity']} 
                                             for sku, info in to_order_nisbets.items()])
            csv_data = nisbets_order_df.to_csv(index=False)
            resp = requests.put(
                f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/root:/Generated/Nisbets_Order.csv:/content",
                headers=headers_csv, data=csv_data.encode('utf-8')
            )
            if resp.status_code >= 300:
                raise HTTPException(status_code=resp.status_code, detail="Failed to upload Nisbets order CSV")
        # Nortons order CSV (if any items need ordering)
        if to_order_nortons:
            nortons_order_df = pd.DataFrame([{"SKU": sku, "Quantity": info['quantity']} 
                                             for sku, info in to_order_nortons.items()])
            csv_data = nortons_order_df.to_csv(index=False)
            resp = requests.put(
                f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/root:/Generated/Nortons_Order.csv:/content",
                headers=headers_csv, data=csv_data.encode('utf-8')
            )
            if resp.status_code >= 300:
                raise HTTPException(status_code=resp.status_code, detail="Failed to upload Nortons order CSV")

        # 7. Prepare a summary of the results to display on screen
        def format_allocation(data_dict):
            return [
                {
                    "SKU": sku,
                    "quantity": info['quantity'],
                    "order_numbers": sorted(info['orders']) if info['orders'] else []
                }
                for sku, info in data_dict.items()
            ]
        summary = {
            "fulfilled_from_stock": format_allocation(fulfilled_from_stock),
            "to_nisbets": format_allocation(to_order_nisbets),
            "to_nortons": format_allocation(to_order_nortons)
        }
        return summary

    except Exception as e:
        # Catch-all for unexpected errors
        raise HTTPException(status_code=500, detail=str(e))


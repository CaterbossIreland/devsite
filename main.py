from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
import pandas as pd
import requests
from io import BytesIO
from datetime import datetime
import uuid

# Import Microsoft Graph helper functions (expects env vars for credentials)
from graph_auth import get_access_token

app = FastAPI()

# Enable CORS (allow all origins for simplicity; adjust as needed for specific domains)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# OneDrive configuration: Drive and Item IDs for stock and supplier files
# (These would typically be set via environment or config for security, not hard-coded)
DRIVE_ID = "b!udRZ7OsrmU61CSAYEn--q1fPtuPR3TZAsv2B9cCW-gzWb8B-lsUaQLURaNYNJxjP"  # e.g., from environment or config
NISBETS_STOCK_FILE_ID = "01YTGSV5HJCNBDXINJP5FJE2TICQ6Q3NEX"
NORTONS_STOCK_FILE_ID = "01YTGSV5FBVS7JYODGLREKL273FSJ3XRLP"
SUPPLIER_FILE_ID = "01YTGSV5ALH67IM5W73JDJ422J6AOUCC6M"

def download_excel_file(item_id: str) -> pd.DataFrame:
    """Download an Excel file from OneDrive by item ID and return as a DataFrame."""
    token = get_access_token()
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{item_id}/content"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        raise HTTPException(status_code=500, detail=f"Failed to download Excel file (ID: {item_id})")
    # Read Excel content into DataFrame
    try:
        df = pd.read_excel(BytesIO(resp.content))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading Excel file ID {item_id}: {e}")
    return df

def download_csv_file(item_id: str) -> pd.DataFrame:
    """Download a CSV file from OneDrive by item ID and return as a DataFrame."""
    token = get_access_token()
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{item_id}/content"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        raise HTTPException(status_code=500, detail=f"Failed to download CSV file (ID: {item_id})")
    try:
        df = pd.read_csv(BytesIO(resp.content))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading CSV file ID {item_id}: {e}")
    return df

def update_excel_file(item_id: str, df: pd.DataFrame):
    """Upload/replace an Excel file on OneDrive (by item ID) with the data from the given DataFrame."""
    token = get_access_token()
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{item_id}/content"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }
    # Write DataFrame to an in-memory Excel file
    with BytesIO() as output:
        df.to_excel(output, index=False)
        output.seek(0)
        resp = requests.put(url, headers=headers, data=output.read())
    if resp.status_code >= 300:
        raise HTTPException(status_code=500, detail=f"Failed to update file ID {item_id} (status {resp.status_code})")

def upload_csv_file(path: str, content: bytes) -> str:
    """
    Upload a CSV file to OneDrive at the given path (e.g. "Generated/filename.csv").
    Returns the DriveItem ID of the uploaded file.
    """
    token = get_access_token()
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/root:/{path}:/content"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "text/csv",
    }
    resp = requests.put(url, headers=headers, data=content)
    if resp.status_code >= 300:
        raise HTTPException(status_code=500, detail=f"Failed to upload CSV to path: {path}")
    # On success, the response should contain the new DriveItem JSON (including the id)
    item = resp.json()
    return item.get("id", "")  # return the new file's item ID

@app.post("/process-order")
async def process_order(file: UploadFile = File(...)):
    """
    Process an uploaded Excel order file:
    - Determine stock availability and supplier for each SKU.
    - Fulfill from stock and prepare supplier orders.
    - Update stock files on OneDrive.
    - Return JSON summary and an ID for the Nisbets order CSV.
    """
    try:
        # Read the uploaded Excel file into a DataFrame
        data = await file.read()
        try:
            orders_df = pd.read_excel(BytesIO(data))
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Could not read Excel file: {e}")
        if orders_df.empty:
            raise HTTPException(status_code=400, detail="Uploaded file is empty or invalid.")

        # Identify the SKU and Quantity columns (case-insensitive match)
        cols_lower = [col.strip().lower() for col in orders_df.columns]
        sku_col = None
        qty_col = None
        # Common names for SKU column (the Musgraves file uses "Offer SKU")
        for col in orders_df.columns:
            if col.strip().lower() in ["offer sku", "product code", "item code", "sku"]:
                sku_col = col
                break
        # Common names for Quantity column (the Musgraves file likely uses "Quantity")
        for col in orders_df.columns:
            if col.strip().lower() in ["quantity", "qty", "qty ordered", "q.ty", "q.ty ordered"]:
                qty_col = col
                break
        if sku_col is None or qty_col is None:
            raise HTTPException(status_code=400, detail="SKU or Quantity column not found in uploaded file.")

        # Rename the identified columns to standard names
        orders_df = orders_df[[sku_col, qty_col]].copy()
        orders_df.columns = ["SKU", "Quantity"]
        # Drop any rows where SKU is missing
        orders_df.dropna(subset=["SKU"], inplace=True)
        # Normalize SKU values (strip whitespace, as strings)
        orders_df["SKU"] = orders_df["SKU"].astype(str).str.strip()
        # Ensure Quantity is numeric (fill NaN with 0 and cast to int)
        orders_df["Quantity"] = orders_df["Quantity"].fillna(0)
        try:
            orders_df["Quantity"] = orders_df["Quantity"].astype(int)
        except ValueError:
            # If any quantity is non-numeric, raise an error
            raise HTTPException(status_code=400, detail="Quantity column contains non-numeric values.")

        # If the same SKU appears multiple times, sum the quantities (group by SKU)
        orders_grouped = orders_df.groupby("SKU", as_index=False)["Quantity"].sum()

        # Load supplier mapping from OneDrive (SKU -> Supplier)
        supplier_df = download_csv_file(SUPPLIER_FILE_ID)
        if "SKU" not in supplier_df.columns or "Supplier" not in supplier_df.columns:
            raise HTTPException(status_code=500, detail="Supplier mapping file is missing required columns.")
        # Create a dictionary for quick lookup of supplier by SKU
        supplier_map = {str(sku).strip(): supplier for sku, supplier in zip(supplier_df["SKU"], supplier_df["Supplier"])}

        # Load current stock data for both suppliers
        nisbets_df = download_excel_file(NISBETS_STOCK_FILE_ID)
        nortons_df = download_excel_file(NORTONS_STOCK_FILE_ID)
        # Ensure SKU and Quantity columns exist in stock files
        for df, name in [(nisbets_df, "Nisbets stock file"), (nortons_df, "Nortons stock file")]:
            if "SKU" not in df.columns or "Quantity" not in df.columns:
                raise HTTPException(status_code=500, detail=f"{name} is missing SKU or Quantity column.")
        # Normalize stock data SKU column and ensure Quantity is int
        nisbets_df["SKU"] = nisbets_df["SKU"].astype(str).str.strip()
        nortons_df["SKU"] = nortons_df["SKU"].astype(str).str.strip()
        nisbets_df["Quantity"] = nisbets_df["Quantity"].fillna(0).astype(int)
        nortons_df["Quantity"] = nortons_df["Quantity"].fillna(0).astype(int)
        # Create dict maps for stock quantities by SKU for quick access
        nisbets_stock = {sku: qty for sku, qty in zip(nisbets_df["SKU"], nisbets_df["Quantity"])}
        nortons_stock = {sku: qty for sku, qty in zip(nortons_df["SKU"], nortons_df["Quantity"])}

        # Prepare result lists
        fulfilled_from_stock = []      # list of dicts: {SKU, quantity_fulfilled}
        to_order_from_nisbets = []     # list of dicts: {SKU, quantity_to_order}
        to_order_from_nortons = []     # list of dicts: {SKU, quantity_to_order}

        # Go through each SKU in the order and allocate stock/order accordingly
        for _, row in orders_grouped.iterrows():
            sku = str(row["SKU"]).strip()
            needed_qty = int(row["Quantity"])
            # Determine supplier for this SKU (default "Unknown" if not mapped)
            supplier = supplier_map.get(sku, "Unknown")
            # Check stock based on supplier
            if supplier.lower() == "nisbets":
                stock_qty = nisbets_stock.get(sku, 0)
            elif supplier.lower() == "nortons":
                stock_qty = nortons_stock.get(sku, 0)
            else:
                # SKU supplier not recognized (no stock file available)
                stock_qty = 0
            stock_qty = int(stock_qty) if pd.notna(stock_qty) else 0

            # Fulfill from stock as much as possible
            from_stock = min(needed_qty, stock_qty)
            to_order = needed_qty - from_stock

            # Record fulfilled from stock if any
            if from_stock > 0:
                fulfilled_from_stock.append({
                    "SKU": sku,
                    "quantity": from_stock
                })
                # Reduce stock quantity for that SKU
                if supplier.lower() == "nisbets":
                    nisbets_stock[sku] = stock_qty - from_stock
                elif supplier.lower() == "nortons":
                    nortons_stock[sku] = stock_qty - from_stock

            # Record items to order from supplier if needed
            if to_order > 0:
                if supplier.lower() == "nisbets":
                    to_order_from_nisbets.append({
                        "SKU": sku,
                        "quantity": to_order
                    })
                elif supplier.lower() == "nortons":
                    to_order_from_nortons.append({
                        "SKU": sku,
                        "quantity": to_order
                    })
                else:
                    # If supplier is unknown or not Nisbets/Nortons, we could handle separately.
                    # For now, treat unknown supplier similar to Nortons (or simply skip if none).
                    to_order_from_nortons.append({
                        "SKU": sku,
                        "quantity": to_order
                    })

        # Update the stock DataFrames with new quantities from the stock maps
        # (Only SKUs that were fulfilled from stock have changed quantities)
        for sku, new_qty in nisbets_stock.items():
            # Find the row in nisbets_df and update it
            mask = nisbets_df["SKU"] == sku
            if mask.any():
                nisbets_df.loc[mask, "Quantity"] = int(new_qty)
        for sku, new_qty in nortons_stock.items():
            mask = nortons_df["SKU"] == sku
            if mask.any():
                nortons_df.loc[mask, "Quantity"] = int(new_qty)

        # Upload the updated stock files back to OneDrive to reflect fulfillment
        try:
            update_excel_file(NISBETS_STOCK_FILE_ID, nisbets_df)
            update_excel_file(NORTONS_STOCK_FILE_ID, nortons_df)
        except Exception as e:
            # If stock file update fails, we still proceed to provide output, but log the error
            raise HTTPException(status_code=500, detail=f"Stock update failed: {e}")

        # Create a CSV for Nisbets order (SKUs to order from Nisbets)
        nisbets_order_df = pd.DataFrame(to_order_from_nisbets)
        # If there are no items to order from Nisbets, we can still create an empty file or handle accordingly
        csv_bytes = nisbets_order_df.to_csv(index=False).encode('utf-8')

        # Use a unique filename for the Nisbets order CSV (to avoid conflicts on OneDrive)
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"Nisbets_Order_{timestamp}.csv"
        csv_path = f"Generated/{csv_filename}"
        # Upload the CSV to OneDrive (in the "Generated" folder) and get its file ID
        try:
            nisbets_csv_id = upload_csv_file(csv_path, csv_bytes)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to upload Nisbets CSV: {e}")
        if not nisbets_csv_id:
            raise HTTPException(status_code=500, detail="Could not retrieve ID for uploaded Nisbets CSV file.")

        # Prepare the JSON response
        response_data = {
            "fulfilled_from_stock": fulfilled_from_stock,
            "to_order_from_nisbets": to_order_from_nisbets,
            "to_order_from_nortons": to_order_from_nortons,
            "nisbets_csv_id": nisbets_csv_id
        }
        return response_data

    except Exception as e:
        # Catch-all for unexpected errors
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/download-nisbets-csv")
def download_nisbets_csv(file_id: str):
    """
    Download the Nisbets order CSV file from OneDrive given its file ID.
    Returns the file content with a CSV mime type.
    """
    token = get_access_token()
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{file_id}/content"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        raise HTTPException(status_code=404, detail="Nisbets order CSV not found or access failed.")
    # Return the CSV content with appropriate headers for download
    return Response(
        content=resp.content,
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="Nisbets_Order.csv"'}
    )

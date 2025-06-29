from fastapi import FastAPI, UploadFile, File, HTTPException
import pandas as pd
import numpy as np
import requests
from io import BytesIO
from datetime import datetime

app = FastAPI()

# === Configuration: Microsoft Graph Credentials and IDs ===
TENANT_ID = "ce280aae-ee92-41fe-ab60-66b37ebc97dd"
CLIENT_ID = "83acd574-ab02-4cfe-b28c-e38c733d9a52"
CLIENT_SECRET = "FYX8Q~bZVXuKEenMTryxYw-ZuQOq2OBTNIu8Qa~i"
DRIVE_ID = "b!udRZ7OsrmU61CSAYEn--q1fPtuPR3TZAsv2B9cCW-gzWb8B-lsUaQLURaNYNJxjP"
NISBETS_STOCK_FILE_ID = "01YTGSV5HJCNBDXINJP5FJE2TICQ6Q3NEX"
NORTONS_STOCK_FILE_ID = "01YTGSV5FBVS7JYODGLREKL273FSJ3XRLP"
SUPPLIER_FILE_ID = "01YTGSV5ALH67IM5W73JDJ422J6AOUCC6M"
STOCK_FILE_IDS = {
    "nisbets": NISBETS_STOCK_FILE_ID,
    "nortons": NORTONS_STOCK_FILE_ID,
}

def get_graph_token() -> str:
    """Obtain an OAuth2 access token for Microsoft Graph using client credentials."""
    token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default"
    }
    resp = requests.post(token_url, data=data)
    if resp.status_code != 200:
        # Include error details if available
        error_detail = ""
        try:
            error_detail = resp.json().get("error_description") or resp.json().get("error", {}).get("message", "")
        except ValueError:
            error_detail = resp.text
        raise Exception(f"Failed to obtain access token (HTTP {resp.status_code}): {error_detail}")
    token = resp.json().get("access_token")
    if not token:
        raise Exception("Authentication succeeded but no access token was returned.")
    return token

def download_supplier_csv(token: str) -> pd.DataFrame:
    """Download the supplier reference CSV from OneDrive using Graph API and return it as a DataFrame."""
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{SUPPLIER_FILE_ID}/content"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        raise Exception(f"Failed to download supplier reference file (HTTP {resp.status_code}).")
    # Read the CSV content into a DataFrame
    content = resp.content  # bytes of the CSV file
    try:
        supplier_df = pd.read_csv(BytesIO(content))
    except Exception as e:
        raise Exception(f"Error reading supplier CSV: {e}")
    return supplier_df

def upload_file_to_onedrive(token: str, drive_id: str, filename: str, file_bytes: bytes):
    """Upload a file (given by bytes) to the specified OneDrive drive (root directory) via Graph API."""
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{filename}:/content"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "text/csv"
    }
    resp = requests.put(url, headers=headers, data=file_bytes)
    if resp.status_code not in (200, 201):
        # If not success, raise an error with Graph's message if available
        error_msg = resp.text
        raise Exception(f"Failed to upload '{filename}' (HTTP {resp.status_code}): {error_msg}")

@app.post("/process-orders/")
async def process_orders(file: UploadFile = File(...)):
    """
    Process an uploaded Excel file of orders and generate separate CSVs for Nisbets and Nortons.
    Expects columns 'Offer SKU', 'Order Number', 'Quantity' exactly.
    """
    # Step 1: Read the uploaded Excel file into a DataFrame
    file_content = await file.read()
    try:
        orders_df = pd.read_excel(BytesIO(file_content), engine="openpyxl")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not read Excel file: {e}")

    # Step 2: Validate that required columns exist exactly as expected
    actual_columns = [col for col in orders_df.columns]  # preserve exact names as provided
    required_columns = ["Offer SKU", "Order Number", "Quantity"]
    missing_cols = [col for col in required_columns if col not in actual_columns]
    if missing_cols:
        # Format found columns in error message
        found_list = ", ".join([f"'{col}'" for col in actual_columns])
        missing_list = ", ".join([f"'{col}'" for col in missing_cols])
        raise HTTPException(
            status_code=400,
            detail=f"Missing required column(s): {missing_list}. Found columns: {found_list}."
        )
    # (Optionally, we could ignore extra columns if present. They will simply be ignored in processing.)

    # Step 3: Download supplier reference CSV to get SKU -> Supplier mapping
    try:
        token = get_graph_token()
    except Exception as auth_err:
        raise HTTPException(status_code=500, detail=f"Authentication failed: {auth_err}")
    try:
        supplier_df = download_supplier_csv(token)
    except Exception as dl_err:
        raise HTTPException(status_code=500, detail=str(dl_err))
    # Ensure supplier file has the expected columns for mapping
    if not {"SKU", "Supplier"}.issubset(set(supplier_df.columns)):
        raise HTTPException(status_code=500, detail="Supplier reference CSV is missing 'SKU' or 'Supplier' column.")

    # Build a dictionary for quick lookup of supplier by SKU (strip SKU strings to normalize lookup)
    supplier_map = {
        str(row["SKU"]).strip(): str(row["Supplier"]).strip()
        for _, row in supplier_df.iterrows() 
        if not pd.isna(row.get("SKU")) and not pd.isna(row.get("Supplier"))
    }

    # Step 4: Separate orders by supplier using the mapping
    orders_df = orders_df.copy()  # work on a copy to avoid SettingWithCopy warnings
    # Create a new column for supplier name using the mapping (unmatched SKUs get NaN which we'll fill as empty)
    orders_df["Supplier"] = orders_df["Offer SKU"].apply(lambda sku: supplier_map.get(str(sku).strip()))
    orders_df["Supplier"] = orders_df["Supplier"].fillna("")  # replace NaN with empty string for safe comparison

    # Filter into two DataFrames: one for Nisbets orders, one for Nortons orders
    nisbets_df = orders_df[orders_df["Supplier"].str.lower() == "nisbets"].copy()
    nortons_df = orders_df[orders_df["Supplier"].str.lower() == "nortons"].copy()

    # Drop the helper 'Supplier' column, we only want original columns in output
    nisbets_df = nisbets_df[required_columns]
    nortons_df = nortons_df[required_columns]

    # Convert Quantity values to int (if whole number) or blank if missing, to avoid any ".0" in CSV
    def format_quantity(val):
        if pd.isna(val):
            return ""  # blank for missing values
        if isinstance(val, (int, np.integer)):
            return int(val)  # ensure it's a plain int
        if isinstance(val, float):
            return int(val) if val.is_integer() else val  # remove .0 if no fractional part
        return val  # if already a string or unexpected type, leave as is

    if not nisbets_df.empty:
        nisbets_df.loc[:, "Quantity"] = nisbets_df["Quantity"].apply(format_quantity)
    if not nortons_df.empty:
        nortons_df.loc[:, "Quantity"] = nortons_df["Quantity"].apply(format_quantity)

    # Step 5: Generate CSV files in memory for each supplier
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    results = {"success": True}
    try:
        if not nisbets_df.empty:
            nisbets_filename = f"Nisbets_Orders_{timestamp}.csv"
            csv_bytes = nisbets_df.to_csv(index=False, na_rep="", encoding="utf-8").encode("utf-8")
            upload_file_to_onedrive(token, DRIVE_ID, nisbets_filename, csv_bytes)
            results["nisbets_csv"] = nisbets_filename
            results["nisbets_count"] = len(nisbets_df)
        if not nortons_df.empty:
            nortons_filename = f"Nortons_Orders_{timestamp}.csv"
            csv_bytes = nortons_df.to_csv(index=False, na_rep="", encoding="utf-8").encode("utf-8")
            upload_file_to_onedrive(token, DRIVE_ID, nortons_filename, csv_bytes)
            results["nortons_csv"] = nortons_filename
            results["nortons_count"] = len(nortons_df)
    except Exception as upload_err:
        # If uploading failed, return server error
        raise HTTPException(status_code=500, detail=f"File upload failed: {upload_err}")

    # Step 6: Return a success response with details of the operation
    return results

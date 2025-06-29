# graph_files.py
import requests
import pandas as pd
from io import BytesIO
from graph_auth import get_access_token

def download_excel_file(drive_id: str, item_id: str) -> pd.DataFrame:
    token = get_access_token()
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/content"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        _handle_graph_error(resp, "download Excel file")
    try:
        df = pd.read_excel(BytesIO(resp.content))
    except Exception as e:
        raise Exception(f"Failed to parse Excel file content: {e}")
    return df

def download_csv_file(drive_id: str, item_id: str) -> pd.DataFrame:
    token = get_access_token()
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/content"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        _handle_graph_error(resp, "download CSV file")
    try:
        df = pd.read_csv(BytesIO(resp.content))
    except Exception as e:
        raise Exception(f"Failed to parse CSV file content: {e}")
    return df

def update_excel_file(drive_id: str, item_id: str, df: pd.DataFrame) -> None:
    token = get_access_token()
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/content"
    headers = {"Authorization": f"Bearer {token}"}
    buffer = BytesIO()
    try:
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False)
    except Exception as e:
        raise Exception(f"Failed to write DataFrame to Excel format: {e}")
    buffer.seek(0)
    resp = requests.put(url, headers=headers, data=buffer.read())
    if resp.status_code not in (200, 201):
        _handle_graph_error(resp, "update Excel file")

def upload_csv_file(drive_id: str, path: str, content: bytes) -> str:
    token = get_access_token()
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{path}:/content"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.put(url, headers=headers, data=content)
    if resp.status_code not in (200, 201):
        _handle_graph_error(resp, "upload CSV file")
    try:
        return resp.json().get("id")
    except ValueError:
        raise Exception("Upload succeeded but response is not valid JSON")

def upload_stock_update(df: pd.DataFrame, updates: dict) -> pd.DataFrame:
    df = df.copy()
    if "SKU" not in df.columns or "QTY" not in df.columns:
        raise Exception("Missing SKU or QTY columns in stock file")
    df.set_index("SKU", inplace=True)
    for sku, qty_used in updates.items():
        if sku in df.index:
            df.at[sku, "QTY"] = max(df.at[sku, "QTY"] - qty_used, 0)
        else:
            df.loc[sku] = [0]  # default to zero if not found
    df.reset_index(inplace=True)
    return df

def _handle_graph_error(response: requests.Response, action: str):
    status = response.status_code
    try:
        message = response.json().get("error", {}).get("message") or response.text
    except ValueError:
        message = response.text or "Unknown error"
    raise Exception(f"Failed to {action} (HTTP {status}): {message}")

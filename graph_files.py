# graph_files.py
import requests
import pandas as pd
from io import BytesIO
from graph_auth import get_access_token

def download_excel_file(drive_id: str, item_id: str) -> pd.DataFrame:
    """Download an Excel file from OneDrive (by item ID) and return it as a pandas DataFrame."""
    token = get_access_token()
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/content"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        _handle_graph_error(resp, "download Excel file")
    # Read Excel content into DataFrame
    data = resp.content  # bytes of the .xlsx file
    try:
        df = pd.read_excel(BytesIO(data))
    except Exception as e:
        raise Exception(f"Failed to parse Excel file content: {e}")
    return df

def download_csv_file(drive_id: str, item_id: str) -> pd.DataFrame:
    """Download a CSV file from OneDrive (by item ID) and return it as a pandas DataFrame."""
    token = get_access_token()
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/content"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        _handle_graph_error(resp, "download CSV file")
    # Read CSV content into DataFrame
    data = resp.content  # bytes of the CSV file
    try:
        df = pd.read_csv(BytesIO(data))
    except Exception as e:
        raise Exception(f"Failed to parse CSV file content: {e}")
    return df

def update_excel_file(drive_id: str, item_id: str, df: pd.DataFrame) -> None:
    """Upload a DataFrame to an existing Excel file on OneDrive (replace file content)."""
    token = get_access_token()
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/content"
    headers = {"Authorization": f"Bearer {token}"}
    # Convert DataFrame to Excel binary
    buffer = BytesIO()
    try:
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False)
    except Exception as e:
        raise Exception(f"Failed to write DataFrame to Excel format: {e}")
    buffer.seek(0)
    excel_bytes = buffer.read()
    # PUT the new content to Graph (replace the file)
    resp = requests.put(url, headers=headers, data=excel_bytes)
    if resp.status_code not in (200, 201):
        _handle_graph_error(resp, "update Excel file")

def upload_csv_file(drive_id: str, path: str, content: bytes) -> str:
    """Upload a new CSV file to OneDrive at the given path. Returns the new item's ID."""
    token = get_access_token()
    # Using the root path syntax to upload; this will create or replace the file at the given path
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{path}:/content"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.put(url, headers=headers, data=content)
    # Graph API returns 201 Created or 200 OK with the DriveItem JSON if successful
    if resp.status_code not in (200, 201):
        _handle_graph_error(resp, "upload CSV file")
    try:
        item = resp.json()
    except ValueError:
        raise Exception("Upload succeeded but did not return valid JSON response")
    new_id = item.get("id")
    if not new_id:
        raise Exception("Upload succeeded but no item ID returned in response")
    return new_id

def _handle_graph_error(response: requests.Response, action: str):
    """Helper to raise an exception with details from a failed Graph API response."""
    status = response.status_code
    try:
        error_json = response.json()
        # Microsoft Graph errors often have a structure: {"error": {"code": "...", "message": "..."}}
        message = error_json.get("error", {}).get("message") or error_json.get("error_description")
    except ValueError:
        message = response.text or "Unknown error"
    raise Exception(f"Failed to {action} (HTTP {status}): {message}")

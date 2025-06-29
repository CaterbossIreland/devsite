# graph_files.py
import requests
import pandas as pd
from io import BytesIO
from graph_auth import get_access_token

SITE_ID = "caterboss.sharepoint.com,798d8a1b-c8b4-493e-b320-be94a4c165a1,ec07bde5-4a37-459a-92ef-a58100f17191"
DRIVE_ID = "b!udRZ7OsrmU61CSAYEn--q1fPtuPR3TZAs"

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
        item = resp.json()
    except ValueError:
        raise Exception("Upload succeeded but did not return valid JSON response")
    new_id = item.get("id")
    if not new_id:
        raise Exception("Upload succeeded but no item ID returned in response")
    return new_id

def read_sheet_data(file_id, sheet_name="Sheet1"):
    token = get_access_token()
    url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/items/{file_id}/workbook/worksheets('{sheet_name}')/usedRange"
    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to read sheet: {response.text}")

    values = response.json().get("values", [])
    if not values or len(values) < 2:
        return pd.DataFrame()

    headers_row = values[0]
    rows = values[1:]
    df = pd.DataFrame(rows, columns=headers_row)
    df.columns = [str(c).strip().upper() for c in df.columns]
    return df

def _handle_graph_error(response: requests.Response, action: str):
    status = response.status_code
    try:
        error_json = response.json()
        message = error_json.get("error", {}).get("message") or error_json.get("error_description")
    except ValueError:
        message = response.text or "Unknown error"
    raise Exception(f"Failed to {action} (HTTP {status}): {message}")

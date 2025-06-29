import requests
from graph_auth import get_access_token

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
USER_ID = "008ae000-382a-4483-b89b-19b2ff510bca"  # your Object ID

def get_excel_file_metadata(filename="Nisbets_Order_List.xlsx"):
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{GRAPH_BASE_URL}/users/{USER_ID}/drive/root:/{filename}"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()["id"]
def list_excel_sheets(file_id):
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{GRAPH_BASE_URL}/users/{USER_ID}/drive/items/{file_id}/workbook/worksheets"
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    return [sheet["name"] for sheet in response.json()["value"]]
def read_sheet_data(file_id: str, sheet_name="Sheet1"):
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}"}

    url = (
        f"{GRAPH_BASE_URL}/users/{USER_ID}/drive/items/{file_id}/"
        f"workbook/worksheets('{sheet_name}')/usedRange(valuesOnly=true)"
    )

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    data = response.json()["values"]  # First row = headers
    if not data or len(data) < 2:
        return []

    headers_row = data[0]
    rows = [dict(zip(headers_row, row)) for row in data[1:]]

    return rows

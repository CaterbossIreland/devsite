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

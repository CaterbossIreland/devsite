from graph_auth import get_access_token
import requests
import os

GRAPH_ROOT = "https://graph.microsoft.com/v1.0"
USER_ID = os.environ.get("USER_ID")  # 👈 You’ll add this to Render

def list_onedrive_root_files():
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    
    url = f"{GRAPH_ROOT}/users/{USER_ID}/drive/root/children"

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    files = response.json()["value"]
    return [{ "name": f["name"], "id": f["id"] } for f in files]

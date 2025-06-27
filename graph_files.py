from graph_auth import get_access_token
import requests

GRAPH_ROOT = "https://graph.microsoft.com/v1.0"

def list_onedrive_root_files():
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    
    url = f"{GRAPH_ROOT}/me/drive/root/children"

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    files = response.json()["value"]
    return [{ "name": f["name"], "id": f["id"] } for f in files]

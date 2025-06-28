import os
import requests
from fastapi import FastAPI

app = FastAPI()

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

GRAPH_SCOPE = "https://graph.microsoft.com/.default"
TOKEN_ENDPOINT = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

@app.get("/")
def root():
    return {"message": "Server is up and running."}

@app.get("/get-token")
def get_token():
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": GRAPH_SCOPE
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    response = requests.post(TOKEN_ENDPOINT, data=data, headers=headers)
    if response.status_code != 200:
        return {"error": response.text}
    
    return response.json()

@app.get("/list-files")
def list_files():
    # Step 1: Get access token
    token_resp = get_token()
    if "access_token" not in token_resp:
        return {"error": "Token fetch failed", "details": token_resp}

    token = token_resp["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    # Step 2: Call Graph API to list files in root of user's OneDrive
    graph_endpoint = "https://graph.microsoft.com/v1.0/me/drive/root/children"
    response = requests.get(graph_endpoint, headers=headers)

    if response.status_code != 200:
        return {"error": response.text}

    return response.json()

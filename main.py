from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

# Function to get OAuth2 token
def get_access_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": CLIENT_ID,
        "scope": "https://graph.microsoft.com/.default",
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials"
    }
    response = requests.post(url, data=data)
    if response.status_code != 200:
        raise HTTPException(status_code=500, detail="Failed to obtain token")
    return response.json()["access_token"]

# Endpoint to list SharePoint sites and get their IDs
@app.get("/list_sites")
def list_sites():
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = "https://graph.microsoft.com/v1.0/sites?search=*"
    response = requests.get(url, headers=headers)
    return response.json()

# Example request model for read/write endpoints
class ExcelFileRequest(BaseModel):
    site_id: str
    drive_id: str
    item_id: str

# READ Excel contents from file
@app.post("/read_excel")
def read_excel(request: ExcelFileRequest):
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}"}

    url = (
        f"https://graph.microsoft.com/v1.0/sites/{request.site_id}/drives/"
        f"{request.drive_id}/items/{request.item_id}/workbook/worksheets"
    )
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.json())
    return response.json()

# WRITE to Excel file (placeholder)
@app.post("/write_excel")
def write_excel(request: ExcelFileRequest):
    token = get_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # This is an example to update cell A1 in the first worksheet
    url = (
        f"https://graph.microsoft.com/v1.0/sites/{request.site_id}/drives/"
        f"{request.drive_id}/items/{request.item_id}/workbook/worksheets('Sheet1')/range(address='A1')"
    )
    body = {
        "values": [["Updated by FastAPI!"]]
    }

    response = requests.patch(url, headers=headers, json=body)
    if response.status_code not in (200, 204):
        raise HTTPException(status_code=response.status_code, detail=response.json())
    return {"message": "Cell A1 updated"}
@app.get("/list_drives")
def list_drives():
    token = get_access_token()
    
    # Replace with your real Site ID from /list_sites
    site_id = "caterboss.sharepoint.com,7c743e5e-cf99-49a2-8f9c-bc7fa3bc70b1,602a9996-a3a9-473c-9817-3f665aff0fe0"
    
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
    headers = {"Authorization": f"Bearer {token}"}
    
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)

    return response.json()


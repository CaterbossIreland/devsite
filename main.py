from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests
import httpx
import os

app = FastAPI()

# === ENVIRONMENT CONFIGURATION ===
TENANT_ID = "ce280aae-ee92-41fe-ab60-66b37ebc97dd"
CLIENT_ID = "83acd574-ab02-4cfe-b28c-e38c733d9a52"
CLIENT_SECRET = "FYX8Q~bZVXuKEenMTryxYw-ZuQOq2OBTNIu8Qa~i"

# === SYNC ACCESS TOKEN FUNCTION ===
def get_access_token_sync():
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

# === ASYNC ACCESS TOKEN FUNCTION (optional use) ===
async def get_access_token_async():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": CLIENT_ID,
        "scope": "https://graph.microsoft.com/.default",
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials"
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    async with httpx.AsyncClient() as client:
        response = await client.post(url, data=data, headers=headers)
        response.raise_for_status()
        return response.json()["access_token"]

# === TEST ENDPOINT TO VERIFY TOKEN WORKS ===
@app.get("/test_token")
async def test_token():
    token = await get_access_token_async()
    return {"access_token": token}

# === GET SHAREPOINT SITES ===
@app.get("/list_sites")
def list_sites():
    token = get_access_token_sync()
    headers = {"Authorization": f"Bearer {token}"}
    url = "https://graph.microsoft.com/v1.0/sites?search=*"
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return response.json()

# === GET DRIVES ON A SHAREPOINT SITE ===
@app.get("/list_drives")
def list_drives():
    token = get_access_token_sync()
    site_id = "caterboss.sharepoint.com,7c743e5e-cf99-49a2-8f9c-bc7fa3bc70b1,602a9996-a3a9-473c-9817-3f665aff0fe0"
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return response.json()

# === GET FILES IN A SPECIFIC DRIVE ROOT ===
@app.get("/list_files")
def list_files():
    token = get_access_token_sync()
    drive_id = "b!Xj5dfJnPokmPnLx_o7xwsZaZKmCpozxHmBc_2Ir_D-BcEXAr8106SpXDV8pjRLut"
    site_id = "caterboss.sharepoint.com,7c743e5e-cf99-49a2-8f9c-bc7fa3bc70b1,602a9996-a3a9-473c-9817-3f665aff0fe0"
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root/children"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return response.json()

# === EXCEL MODEL FOR FILE TARGETING ===
class ExcelFileRequest(BaseModel):
    site_id: str
    drive_id: str
    item_id: str

# === READ EXCEL WORKSHEETS ===
@app.post("/read_excel")
def read_excel(request: ExcelFileRequest):
    token = get_access_token_sync()
    headers = {"Authorization": f"Bearer {token}"}
    url = (
        f"https://graph.microsoft.com/v1.0/sites/{request.site_id}/drives/"
        f"{request.drive_id}/items/{request.item_id}/workbook/worksheets"
    )
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.json())
    return response.json()

# === WRITE TO CELL A1 IN SHEET1 ===
@app.post("/write_excel")
def write_excel(request: ExcelFileRequest):
    token = get_access_token_sync()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
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

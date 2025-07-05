import os
import json
import pandas as pd
from datetime import datetime
from fastapi import FastAPI, File, UploadFile, Form, Request, Depends
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
import requests
from io import BytesIO, StringIO

# ---- CONFIG ----
ADMIN_PASSWORD = "Orendaent101!"
ORDER_HISTORY_FILE_ID = "01YTGSV5BZ2T4AVNGCU5F3EWLPXYMKFATG"   # <- Update if changed!
SKU_MAX_FILE_ID = "01YTGSV5DOW27RMJGS3JA2IODH6HCF4647"         # <- Update if changed!
SUPPLIER_FILE_ID = "01YTGSV5DGZEMEISWEYVDJRULO4ADDVCVQ"
NISBETS_STOCK_FILE_ID = "01YTGSV5GERF436HITURGITCR3M7XMYJHF"
NORTONS_STOCK_FILE_ID = "01YTGSV5FKHUI4S6BVWJDLNWETK4TUU26D"
DRIVE_ID = os.getenv("DRIVE_ID", "b!udRZ7OsrmU61CSAYEn--q1fPtuPR3TZAsv2B9cCW-gzWb8B-lsUaQLURaNYNJxjP")
TENANT_ID = os.getenv("TENANT_ID", "ce280aae-ee92-41fe-ab60-66b37ebc97dd")
CLIENT_ID = os.getenv("CLIENT_ID", "83acd574-ab02-4cfe-b28c-e38c733d9a52")
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "FYX8Q~bZVXuKEenMTryxYw-ZuQOq2OBTNIu8Qa~i")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
app.add_middleware(SessionMiddleware, secret_key="REPLACE_THIS_WITH_SOMETHING_RANDOM")

templates = Jinja2Templates(directory="templates")

# ------------- Auth -------------
def is_logged_in(request: Request):
    return request.session.get("admin") == True

def require_admin(request: Request):
    if not is_logged_in(request):
        return RedirectResponse("/admin_login", status_code=302)

# ------------- MS Graph Helper -------------
def get_graph_access_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default"
    }
    r = requests.post(url, data=data)
    r.raise_for_status()
    return r.json()['access_token']

def download_onedrive_file(file_id):
    token = get_graph_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{file_id}/content"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.content

def upload_onedrive_file(file_id, bytes_data, mime):
    token = get_graph_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": mime,
    }
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{file_id}/content"
    r = requests.put(url, headers=headers, data=bytes_data)
    r.raise_for_status()

# ------------- SKU Max Helper -------------
def load_max_per_parcel_map():
    try:
        content = download_onedrive_file(SKU_MAX_FILE_ID)
        return json.loads(content.decode())
    except Exception:
        return {}

def save_max_per_parcel_map(data):
    content = json.dumps(data, indent=2).encode()
    upload_onedrive_file(SKU_MAX_FILE_ID, content, "application/json")

# ------------- Order History Helper -------------
def read_order_history():
    try:
        content = download_onedrive_file(ORDER_HISTORY_FILE_ID)
        return pd.read_csv(BytesIO(content))
    except Exception:
        return pd.DataFrame()

def append_to_order_history(new_orders_df):
    try:
        content = download_onedrive_file(ORDER_HISTORY_FILE_ID)
        old_df = pd.read_csv(BytesIO(content))
        full_df = pd.concat([old_df, new_orders_df], ignore_index=True)
    except Exception:
        full_df = new_orders_df
    buffer = BytesIO()
    full_df.to_csv(buffer, index=False)
    buffer.seek(0)
    upload_onedrive_file(ORDER_HISTORY_FILE_ID, buffer.read(), "text/csv")

# ------------- ADMIN ROUTES -------------
@app.get("/admin_login", response_class=HTMLResponse)
async def admin_login_form(request: Request):
    return templates.TemplateResponse("admin_login.html", {"request": request, "error": ""})

@app.post("/admin_login", response_class=HTMLResponse)
async def admin_login(request: Request):
    form = await request.form()
    if form.get("password") == ADMIN_PASSWORD:
        request.session["admin"] = True
        return RedirectResponse("/admin", status_code=302)
    return templates.TemplateResponse("admin_login.html", {"request": request, "error": "Wrong password"})

@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/", status_code=302)

@app.get("/admin", response_class=HTMLResponse)
async def admin_home(request: Request):
    if not is_logged_in(request): return RedirectResponse("/admin_login", status_code=302)
    return templates.TemplateResponse("admin_home.html", {"request": request})

@app.get("/upload_history", response_class=HTMLResponse)
async def upload_history(request: Request):
    if not is_logged_in(request): return RedirectResponse("/admin_login", status_code=302)
    try:
        content = download_onedrive_file(ORDER_HISTORY_FILE_ID)
        df = pd.read_csv(BytesIO(content))
        uploads = df[["Upload File", "Upload Time"]].drop_duplicates().tail(100).to_dict(orient="records")
    except Exception:
        uploads = []
    return templates.TemplateResponse("upload_history.html", {"request": request, "uploads": uploads})

@app.get("/admin_settings", response_class=HTMLResponse)
async def admin_settings(request: Request):
    if not is_logged_in(request): return RedirectResponse("/admin_login", status_code=302)
    max_map = load_max_per_parcel_map()
    return templates.TemplateResponse("admin_settings.html", {"request": request, "sku_max": max_map, "msg": ""})

@app.post("/set_max_per_parcel", response_class=HTMLResponse)
async def set_max_per_parcel(request: Request, sku: str = Form(...), max_qty: int = Form(...)):
    if not is_logged_in(request): return RedirectResponse("/admin_login", status_code=302)
    max_map = load_max_per_parcel_map()
    max_map[sku.strip()] = int(max_qty)
    save_max_per_parcel_map(max_map)
    msg = f"<b>{sku}:</b> max per parcel set to <b>{max_qty}</b> (saved)."
    return templates.TemplateResponse("admin_settings.html", {"request": request, "msg": msg, "sku_max": {}})  # <-- Clear display after refresh

@app.post("/undo_stock_update", response_class=HTMLResponse)
async def undo_stock_update(request: Request):
    if not is_logged_in(request): return RedirectResponse("/admin_login", status_code=302)
    token = get_graph_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    msgs = []
    for label, file_id in [("Nisbets", NISBETS_STOCK_FILE_ID), ("Nortons", NORTONS_STOCK_FILE_ID)]:
        url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{file_id}/versions"
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        versions = r.json().get('value', [])
        if len(versions) < 2:
            msgs.append(f"<b>{label}:</b> No previous version found.")
            continue
        prev_version_id = versions[1]['id']
        restore_url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{file_id}/versions/{prev_version_id}/restoreVersion"
        r2 = requests.post(restore_url, headers=headers)
        if r2.status_code == 204:
            msgs.append(f"<b>{label}:</b> Restored previous version.")
        else:
            msgs.append(f"<b>{label}:</b> Failed to restore: {r2.text}")
    return templates.TemplateResponse("admin_settings.html", {"request": request, "msg": "<br>".join(msgs), "sku_max": load_max_per_parcel_map()})

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    if not is_logged_in(request): return RedirectResponse("/admin_login", status_code=302)
    df = read_order_history()
    # SKUs
    top_10 = df.groupby("Offer SKU").agg({"Quantity":"sum"}).sort_values("Quantity", ascending=False).head(10).reset_index()
    top_50 = df.groupby("Offer SKU").agg({"Quantity":"sum"}).sort_values("Quantity", ascending=False).head(50).reset_index()
    top_100 = df.groupby("Offer SKU").agg({"Quantity":"sum"}).sort_values("Quantity", ascending=False).head(100).reset_index()
    # Customers
    if "Customer Name" in df.columns:
        top_10_customers = df.groupby("Customer Name").size().sort_values(ascending=False).head(10).reset_index(name='Count')
        top_50_customers = df.groupby("Customer Name").size().sort_values(ascending=False).head(50).reset_index(name='Count')
        top_100_customers = df.groupby("Customer Name").size().sort_values(ascending=False).head(100).reset_index(name='Count')
    else:
        top_10_customers = top_50_customers = top_100_customers = pd.DataFrame()
    # Metrics
    order_count = df["Order number"].nunique() if "Order number" in df.columns else 0
    highest_weekly = df.groupby(pd.to_datetime(df["Upload Time"]).dt.isocalendar().week).size().max() if "Upload Time" in df.columns else 0
    highest_monthly = df.groupby(pd.to_datetime(df["Upload Time"]).dt.month).size().max() if "Upload Time" in df.columns else 0
    highest_daily = df.groupby(pd.to_datetime(df["Upload Time"]).dt.date).size().max() if "Upload Time" in df.columns else 0
    total_sales = df["Quantity"].sum() if "Quantity" in df.columns else 0
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "top_10": top_10.to_dict(orient="records"),
        "top_50": top_50.to_dict(orient="records"),
        "top_100": top_100.to_dict(orient="records"),
        "top_10_customers": top_10_customers.to_dict(orient="records"),
        "top_50_customers": top_50_customers.to_dict(orient="records"),
        "top_100_customers": top_100_customers.to_dict(orient="records"),
        "order_count": order_count,
        "highest_weekly": highest_weekly,
        "highest_monthly": highest_monthly,
        "highest_daily": highest_daily,
        "total_sales": total_sales,
    })

@app.get("/download_history")
async def download_history(request: Request):
    if not is_logged_in(request): return RedirectResponse("/admin_login", status_code=302)
    content = download_onedrive_file(ORDER_HISTORY_FILE_ID)
    return StreamingResponse(BytesIO(content), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=OrderHistory.csv"})

# ------------- MAIN ROUTES -------------
@app.get("/", response_class=HTMLResponse)
async def main_upload_form(request: Request):
    return templates.TemplateResponse("main_upload.html", {"request": request})

@app.post("/upload_orders/display", response_class=HTMLResponse)
async def upload_orders_display(request: Request, file: UploadFile = File(...)):
    df = pd.read_excel(file.file) if file.filename.endswith("xlsx") else pd.read_csv(file.file)
    nowstr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["Upload File"] = file.filename
    df["Upload Time"] = nowstr
    append_to_order_history(df)
    # Insert your existing order processing here if you wish (supplier list, stock checks, etc)
    return templates.TemplateResponse("upload_result.html", {"request": request, "msg": f"Order file '{file.filename}' uploaded and processed."})


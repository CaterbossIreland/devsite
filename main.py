# ========== SECTION 1: Imports, Configs, App Setup ==========
import os
import requests
import pandas as pd
from fastapi import FastAPI, File, UploadFile, Form, Request, Response, status
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from typing import Optional
from datetime import datetime
from io import BytesIO, StringIO
import json

# --- OneDrive/Graph Configs ---
TENANT_ID = os.getenv("TENANT_ID", "ce280aae-ee92-41fe-ab60-66b37ebc97dd")
CLIENT_ID = os.getenv("CLIENT_ID", "83acd574-ab02-4cfe-b28c-e38c733d9a52")
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "FYX8Q~bZVXuKEenMTryxYw-ZuQOq2OBTNIu8Qa~i")
DRIVE_ID = os.getenv("DRIVE_ID", "b!udRZ7OsrmU61CSAYEn--q1fPtuPR3TZAsv2B9cCW-gzWb8B-lsUaQLURaNYNJxjP")
SUPPLIER_FILE_ID = os.getenv("SUPPLIER_FILE_ID", "01YTGSV5DGZEMEISWEYVDJRULO4ADDVCVQ")
NISBETS_STOCK_FILE_ID = os.getenv("NISBETS_STOCK_FILE_ID", "01YTGSV5GERF436HITURGITCR3M7XMYJHF")
NORTONS_STOCK_FILE_ID = os.getenv("NORTONS_STOCK_FILE_ID", "01YTGSV5FKHUI4S6BVWJDLNWETK4TUU26D")
ORDER_HISTORY_FILE_ID = os.getenv("ORDER_HISTORY_FILE_ID", "01YTGSV5BZ2T4AVNGCU5F3EWLPXYMKFATG")
SKU_MAX_FILE_ID = os.getenv("SKU_MAX_FILE_ID", "01YTGSV5DOW27RMJGS3JA2IODH6HCF4647")
UPLOAD_LOG_FILE_ID = os.getenv("UPLOAD_LOG_FILE_ID", "01YTGSV5GJJRXXXWMWPRHKYWSK4K4P3WLC")

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="ultra-secret-CHANGE-THIS")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")
# ========== SECTION 2: Helper Functions ==========

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

def download_csv_file(file_id):
    token = get_graph_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{file_id}/content"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return pd.read_csv(BytesIO(r.content))

def upload_csv_file(file_id, df):
    token = get_graph_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "text/csv"
    }
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{file_id}/content"
    r = requests.put(url, headers=headers, data=csv_buffer.getvalue().encode('utf-8'))
    r.raise_for_status()

def download_excel_file(file_id):
    token = get_graph_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{file_id}/content"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return pd.read_excel(BytesIO(r.content))

def upload_excel_file(file_id, df):
    token = get_graph_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    }
    excel_buffer = BytesIO()
    df.to_excel(excel_buffer, index=False)
    excel_buffer.seek(0)
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{file_id}/content"
    r = requests.put(url, headers=headers, data=excel_buffer.read())
    r.raise_for_status()

def download_json_file(file_id):
    token = get_graph_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{file_id}/content"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return json.loads(r.content.decode())

def upload_json_file(file_id, data):
    token = get_graph_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{file_id}/content"
    r = requests.put(url, headers=headers, data=json.dumps(data))
    r.raise_for_status()

def append_upload_log(filename, dt):
    try:
        log = download_json_file(UPLOAD_LOG_FILE_ID)
        if not isinstance(log, list): log = []
    except Exception:
        log = []
    log.append({"filename": filename, "datetime": dt})
    upload_json_file(UPLOAD_LOG_FILE_ID, log)

def get_upload_log():
    try:
        return download_json_file(UPLOAD_LOG_FILE_ID)
    except Exception:
        return []

# ========== END SECTION 2 ==========
# ========== SECTION 3: Admin Routes ==========

ADMIN_PASSWORD = "Orendaent101!"

def is_admin(request: Request):
    return request.session.get("admin_logged_in", False)

@app.get("/admin/login")
def admin_login_page(request: Request):
    return templates.TemplateResponse("admin.html", {"request": request, "error": None})

@app.post("/admin/login")
async def admin_login(request: Request, password: str = Form(...)):
    if password == ADMIN_PASSWORD:
        request.session["admin_logged_in"] = True
        return RedirectResponse(url="/admin", status_code=302)
    else:
        return templates.TemplateResponse("admin.html", {"request": request, "error": "Incorrect password."})

@app.get("/admin/logout")
def admin_logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/admin/login", status_code=302)

@app.get("/admin")
def admin_dashboard(request: Request):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)
    try:
        order_history = download_csv_file(ORDER_HISTORY_FILE_ID)
    except Exception:
        order_history = pd.DataFrame()
    total_orders = len(order_history)
    total_qty = order_history['Quantity'].sum() if not order_history.empty else 0
    order_history['Order Date'] = pd.to_datetime(order_history['Order Date'], errors='coerce')
    order_history['Customer Name'] = order_history.get('Customer Name', "")
    # Top SKUs and customers (top10 default, can extend)
    top_skus = order_history.groupby('Offer SKU')['Quantity'].sum().reset_index().sort_values("Quantity", ascending=False).head(10)
    top_customers = order_history.groupby('Customer Name')['Quantity'].sum().reset_index().sort_values("Quantity", ascending=False).head(10)
    return templates.TemplateResponse(
        "admin.html",
        {
            "request": request,
            "total_orders": total_orders,
            "total_qty": total_qty,
            "top_skus": top_skus.values.tolist(),
            "top_customers": top_customers.values.tolist(),
        }
    )

# --- Admin settings: Undo last order, max SKU per parcel ---
@app.get("/admin/settings")
def admin_settings(request: Request):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)
    # Show current max per parcel settings
    try:
        max_settings = download_json_file(SKU_MAX_FILE_ID)
    except Exception:
        max_settings = {}
    return templates.TemplateResponse("admin_settings.html", {"request": request, "sku_max": max_settings})

@app.post("/admin/set_max_per_parcel")
async def set_max_per_parcel(request: Request, sku: str = Form(...), max_qty: int = Form(...)):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)
    try:
        settings = download_json_file(SKU_MAX_FILE_ID)
    except Exception:
        settings = {}
    settings[sku] = max_qty
    upload_json_file(SKU_MAX_FILE_ID, settings)
    return RedirectResponse(url="/admin/settings", status_code=302)

@app.post("/admin/undo_last_order")
async def undo_last_order(request: Request):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)
    # Remove last upload from OrderHistory (remove latest set of rows)
    order_hist = download_csv_file(ORDER_HISTORY_FILE_ID)
    upload_log = get_upload_log()
    if upload_log:
        last = upload_log[-1]
        last_filename = last['filename']
        # If possible, filter out those orders by a unique column (e.g. batch/timestamp)
        # For now, drop most recent N rows = count in that batch
        # TODO: improve to be more robust for real use
        order_hist = order_hist.iloc[:-len(order_hist[order_hist['Source Filename'] == last_filename])]
        upload_csv_file(ORDER_HISTORY_FILE_ID, order_hist)
        upload_log = upload_log[:-1]
        upload_json_file(UPLOAD_LOG_FILE_ID, upload_log)
    return RedirectResponse(url="/admin/settings", status_code=302)

# --- Upload history route ---
@app.get("/admin/upload_history")
def upload_history(request: Request):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)
    log = get_upload_log()
    return templates.TemplateResponse("upload_history.html", {"request": request, "upload_log": log})

# ========== END SECTION 3 ==========
# ========== SECTION 4: Main Upload, Order, and Supplier File Logic ==========

@app.get("/", response_class=HTMLResponse)
def main_upload_form(request: Request):
    return templates.TemplateResponse("main_upload.html", {"request": request})

@app.post("/upload_orders/display")
async def upload_orders_display(request: Request, file: UploadFile = File(...)):
    try:
        df = pd.read_excel(file.file) if file.filename.lower().endswith("xlsx") else pd.read_csv(file.file)
        if not set(['Order number', 'Offer SKU', 'Quantity', 'Customer Name']).issubset(df.columns):
            raise Exception("Missing columns: must have 'Order number', 'Offer SKU', 'Quantity', 'Customer Name'")
        orders = df[['Order number', 'Offer SKU', 'Quantity', 'Customer Name']].dropna()
    except Exception as e:
        return HTMLResponse(f"<b>Order file read failed or missing columns:</b> {e}", status_code=500)

    # --- SUPPLIER LOOKUP ---
    try:
        supplier_df = download_csv_file(SUPPLIER_FILE_ID)
        sku_to_supplier = dict(zip(supplier_df['Offer SKU'], supplier_df['Supplier Name']))
        orders['Supplier Name'] = orders['Offer SKU'].map(sku_to_supplier)
    except Exception as e:
        return HTMLResponse(f"<b>Supplier fetch/mapping failed:</b> {e}", status_code=500)

    # --- STOCK LOOKUP ---
    try:
        nisbets_stock = download_excel_file(NISBETS_STOCK_FILE_ID)
        nortons_stock = download_excel_file(NORTONS_STOCK_FILE_ID)
        stock_map = {
            'Nisbets': nisbets_stock.set_index('Offer SKU')['Quantity'].to_dict(),
            'Nortons': nortons_stock.set_index('Offer SKU')['Quantity'].to_dict(),
        }
    except Exception as e:
        return HTMLResponse(f"<b>Stock file fetch failed:</b> {e}", status_code=500)

    # --- MAX SKUS PER PARCEL RULE ---
    try:
        sku_max_settings = download_json_file(SKU_MAX_FILE_ID)
    except Exception:
        sku_max_settings = {}

    def calc_need_to_order(row):
        stock = stock_map.get(row['Supplier Name'], {}).get(row['Offer SKU'], 0)
        max_per = sku_max_settings.get(row['Offer SKU'])
        qty = row['Quantity']
        if max_per and qty > max_per:
            return qty  # For now just show full; can split across parcels in label step
        return max(0, qty - stock)

    orders['Need To Order'] = orders.apply(calc_need_to_order, axis=1)
    orders = orders[orders['Need To Order'] > 0]

    # --- SPLIT FILES FOR SUPPLIERS ---
    for supplier in ['Nisbets', 'Nortons']:
        supplier_orders = orders[orders['Supplier Name'] == supplier]
        if not supplier_orders.empty:
            filename = f"{supplier}_Order_List.xlsx"
            output = BytesIO()
            supplier_orders[['Order number', 'Offer SKU', 'Quantity', 'Customer Name']].to_excel(output, index=False)
            output.seek(0)
            # [Optional: save for download, or email as needed]

    # --- DPD LABEL PREP: GROUP PARCELS ---
    orders['Parcel Count'] = 1
    dpd_labels = orders.groupby('Order number').agg({
        'Parcel Count': 'sum',
        'Customer Name': 'first'
    }).reset_index()
    # [Optionally allow CSV download]

    # --- SAVE TO ORDER HISTORY CSV + UPLOAD LOG ---
    try:
        old_hist = download_csv_file(ORDER_HISTORY_FILE_ID)
        new_hist = pd.concat([old_hist, df], ignore_index=True)
        upload_csv_file(ORDER_HISTORY_FILE_ID, new_hist)
        append_upload_log(file.filename)
    except Exception:
        upload_csv_file(ORDER_HISTORY_FILE_ID, df)
        append_upload_log(file.filename)

    return HTMLResponse("<b>Order processed, supplier orders created, and order history updated!</b>")

# --- DPD Label CSV Download Endpoint ---
@app.get("/dpd_labels.csv")
def dpd_labels_csv():
    orders = download_csv_file(ORDER_HISTORY_FILE_ID)
    orders['Parcel Count'] = 1
    dpd_labels = orders.groupby('Order number').agg({
        'Parcel Count': 'sum',
        'Customer Name': 'first'
    }).reset_index()
    csv_buffer = StringIO()
    dpd_labels.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    return StreamingResponse(csv_buffer, media_type='text/csv', headers={"Content-Disposition": "attachment; filename=dpd_labels.csv"})

# --- SUPPLIER ORDER XLSX DOWNLOAD ---
@app.get("/supplier_orders/{supplier}.xlsx")
def supplier_order_xlsx(supplier: str):
    orders = download_csv_file(ORDER_HISTORY_FILE_ID)
    supplier_orders = orders[orders['Supplier Name'].str.lower() == supplier.lower()]
    output = BytesIO()
    supplier_orders[['Order number', 'Offer SKU', 'Quantity', 'Customer Name']].to_excel(output, index=False)
    output.seek(0)
    return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={
        "Content-Disposition": f"attachment; filename={supplier}_Order_List.xlsx"
    })

# ========== END SECTION 4 ==========
# ========== SECTION 5: Upload Log Utilities, Undo, and END OF FILE ==========

# ---- Append an entry to upload_log.json file in OneDrive ----
def append_upload_log(filename):
    import pytz
    from datetime import datetime
    try:
        log = download_json_file(UPLOAD_LOG_FILE_ID)
    except Exception:
        log = []
    log.append({
        "filename": filename,
        "datetime": datetime.now(pytz.timezone("Europe/Dublin")).isoformat(timespec="seconds")
    })
    upload_json_file(UPLOAD_LOG_FILE_ID, log)

# ---- View Upload Log for Admin ----
@app.get("/admin/upload_log")
def view_upload_log(request: Request):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)
    try:
        log = download_json_file(UPLOAD_LOG_FILE_ID)
    except Exception:
        log = []
    return templates.TemplateResponse("upload_log.html", {"request": request, "log": log})

# ---- Undo Last Order History Update (removes last uploaded batch) ----
@app.post("/admin/undo_last_upload")
def undo_last_upload(request: Request):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)
    try:
        hist = download_csv_file(ORDER_HISTORY_FILE_ID)
        log = download_json_file(UPLOAD_LOG_FILE_ID)
        if not log or hist.empty:
            return HTMLResponse("No upload to undo.", status_code=400)
        last_upload = log[-1]
        last_fname = last_upload["filename"]
        # Remove all rows with this file's orders, assuming all from last batch have same upload time/file.
        # (Adjust logic as needed based on your data structure.)
        # Here we just remove the last N rows as a fallback:
        rows_to_remove = len(hist) // len(log) if len(log) else 1
        hist = hist.iloc[:-rows_to_remove] if rows_to_remove < len(hist) else pd.DataFrame(hist.columns)
        upload_csv_file(ORDER_HISTORY_FILE_ID, hist)
        log = log[:-1]
        upload_json_file(UPLOAD_LOG_FILE_ID, log)
        return RedirectResponse(url="/admin", status_code=302)
    except Exception as e:
        return HTMLResponse(f"Undo failed: {e}", status_code=500)

# ---- Set Max SKU Per Parcel (Update JSON in OneDrive) ----
@app.post("/admin/set_max_per_parcel")
async def set_max_per_parcel(request: Request, sku: str = Form(...), max_qty: int = Form(...)):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)
    try:
        max_settings = download_json_file(SKU_MAX_FILE_ID)
    except Exception:
        max_settings = {}
    max_settings[sku] = max_qty
    upload_json_file(SKU_MAX_FILE_ID, max_settings)
    return RedirectResponse(url="/admin", status_code=302)

# ---- Remove SKU Max Per Parcel Limit ----
@app.post("/admin/remove_max_per_parcel")
async def remove_max_per_parcel(request: Request, sku: str = Form(...)):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)
    try:
        max_settings = download_json_file(SKU_MAX_FILE_ID)
        if sku in max_settings:
            del max_settings[sku]
            upload_json_file(SKU_MAX_FILE_ID, max_settings)
    except Exception:
        pass
    return RedirectResponse(url="/admin", status_code=302)

# ========== END OF FILE ==========

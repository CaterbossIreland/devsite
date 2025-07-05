# ========== SECTION 1: Imports, Configs, App Setup, Helper Functions ==========

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

# --- App & Templates ---
app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="ultra-secret-CHANGE-THIS")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# --- Globals for "current session" files (for supplier downloads etc) ---
latest_nisbets_csv = None
latest_zoho_xlsx = None
latest_dpd_csv = None
dpd_error_report_html = ""

# --- Helper functions for Graph API file ops ---

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
    if not r.content or r.content.strip() in [b'', b'{}']:
        return {}
    return json.loads(r.content.decode("utf-8"))

def upload_json_file(file_id, data):
    token = get_graph_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{file_id}/content"
    r = requests.put(url, headers=headers, data=json.dumps(data).encode("utf-8"))
    r.raise_for_status()

# -- Helpers for max SKU per parcel --
def load_max_per_parcel_map():
    return download_json_file(SKU_MAX_FILE_ID)
def save_max_per_parcel_map(data):
    upload_json_file(SKU_MAX_FILE_ID, data)

# -- Helper for Upload Log --
def append_upload_log(log_dict):
    try:
        log = download_json_file(UPLOAD_LOG_FILE_ID)
    except Exception:
        log = []
    log.append(log_dict)
    upload_json_file(UPLOAD_LOG_FILE_ID, log)

# -- Helper for restoring previous stock version --
def restore_prev_version(file_id):
    token = get_graph_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{file_id}/versions"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    versions = r.json().get('value', [])
    if len(versions) < 2:
        return False, "No previous version found."
    prev_version_id = versions[1]['id']
    restore_url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{file_id}/versions/{prev_version_id}/restoreVersion"
    r2 = requests.post(restore_url, headers=headers)
    if r2.status_code == 204:
        return True, "Restored previous version."
    else:
        return False, f"Failed to restore: {r2.text}"

# ========== END SECTION 1 ==========
# ========== SECTION 2: Admin Routes, Dashboard, Undo, Max Per Parcel, Upload Log ==========

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
def admin_dashboard(request: Request, top: Optional[int] = 10):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)
    try:
        order_history = download_csv_file(ORDER_HISTORY_FILE_ID)
    except Exception:
        order_history = pd.DataFrame()
    if not order_history.empty:
        order_history['Order Date'] = pd.to_datetime(order_history.get('Order Date', pd.NaT), errors='coerce')
        order_history['Customer Name'] = order_history.get('Customer Name', "")
        order_history['Quantity'] = pd.to_numeric(order_history.get('Quantity', 0), errors='coerce').fillna(0)
    total_orders = len(order_history)
    total_qty = int(order_history['Quantity'].sum()) if not order_history.empty else 0
    total_customers = order_history['Customer Name'].nunique() if not order_history.empty else 0
    top_n = int(top) if top else 10
    top_skus = (order_history.groupby('Offer SKU')['Quantity'].sum()
                .sort_values(ascending=False).reset_index().head(top_n).values.tolist() if not order_history.empty else [])
    top_customers = (order_history.groupby('Customer Name')['Quantity'].sum()
                     .sort_values(ascending=False).reset_index().head(top_n).values.tolist() if not order_history.empty else [])
    total_sales = total_qty * 1  # adjust with your price logic
    # Add more stats as needed

    # --- Read upload log ---
    try:
        upload_log = download_json_file(UPLOAD_LOG_FILE_ID)
    except Exception:
        upload_log = []

    return templates.TemplateResponse(
        "admin.html",
        {
            "request": request,
            "total_orders": total_orders,
            "total_qty": total_qty,
            "total_customers": total_customers,
            "top_skus": top_skus,
            "top_customers": top_customers,
            "upload_log": upload_log,
            "top_n": top_n,
        }
    )

@app.post("/undo_stock_update")
async def undo_stock_update(request: Request):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)
    msgs = []
    for label, file_id in [("Nisbets", NISBETS_STOCK_FILE_ID), ("Nortons", NORTONS_STOCK_FILE_ID)]:
        success, msg = restore_prev_version(file_id)
        msgs.append(f"<b>{label}:</b> {msg}")
    return HTMLResponse("<br>".join(msgs))

@app.post("/set_max_per_parcel")
async def set_max_per_parcel(request: Request, sku: str = Form(...), max_qty: int = Form(...)):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)
    sku = sku.strip()
    max_per_parcel_map = load_max_per_parcel_map()
    max_per_parcel_map[sku] = int(max_qty)
    save_max_per_parcel_map(max_per_parcel_map)
    return HTMLResponse(f"<b>{sku}:</b> max per parcel set to <b>{max_qty}</b> (saved).")

@app.get("/upload_history")
def upload_history(request: Request):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=302)
    try:
        upload_log = download_json_file(UPLOAD_LOG_FILE_ID)
    except Exception:
        upload_log = []
    rows = "".join(
        f"<tr><td>{x.get('filename')}</td><td>{x.get('upload_time')}</td></tr>"
        for x in upload_log
    )
    html = f"""
    <h2>Order File Upload History</h2>
    <table border=1 cellpadding=6>
        <tr><th>Filename</th><th>Uploaded at</th></tr>
        {rows}
    </table>
    """
    return HTMLResponse(html)

# ========== END SECTION 2 ==========
# ========== SECTION 3: Main Upload Form, Order Processing, OrderHistory, Downloads ==========

from datetime import datetime

@app.get("/", response_class=HTMLResponse)
async def main_upload_form(request: Request):
    max_per_parcel_map = load_max_per_parcel_map()
    rules_html = ""
    if max_per_parcel_map:
        rules_html = "<div style='background:#eef4fc;padding:1em 1.5em;border-radius:8px;margin-bottom:1em;'><b>Current max per parcel settings:</b><ul>"
        for sku, maxqty in max_per_parcel_map.items():
            rules_html += f"<li><b>{sku}</b>: {maxqty}</li>"
        rules_html += "</ul></div>"
    return templates.TemplateResponse("main_upload.html", {"request": request, "rules_html": rules_html})

@app.post("/upload_orders/display")
async def upload_orders_display(request: Request, file: UploadFile = File(...)):
    # --- Log upload ---
    try:
        upload_log = download_json_file(UPLOAD_LOG_FILE_ID)
    except Exception:
        upload_log = []
    upload_log.append({
        "filename": file.filename,
        "upload_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })
    upload_json_file(UPLOAD_LOG_FILE_ID, upload_log)

    max_per_parcel_map = load_max_per_parcel_map()
    try:
        df = pd.read_excel(file.file) if file.filename.lower().endswith("xlsx") else pd.read_csv(file.file)
        orders = df[['Order number', 'Offer SKU', 'Quantity']].dropna()
    except Exception as e:
        return HTMLResponse(f"<b>Order file read failed or missing columns:</b> {e}", status_code=500)

    try:
        supplier_df = download_supplier_csv()
        sku_to_supplier = dict(zip(supplier_df['Offer SKU'], supplier_df['Supplier Name']))
        orders['Supplier Name'] = orders['Offer SKU'].map(sku_to_supplier)
    except Exception as e:
        return HTMLResponse(f"<b>Supplier fetch/mapping failed:</b> {e}", status_code=500)

    try:
        nisbets_stock = download_excel_file(NISBETS_STOCK_FILE_ID)
        nortons_stock = download_excel_file(NORTONS_STOCK_FILE_ID)
        stock_map = {
            'Nisbets': nisbets_stock.set_index('Offer SKU')['Quantity'].to_dict(),
            'Nortons': nortons_stock.set_index('Offer SKU')['Quantity'].to_dict(),
        }
    except Exception as e:
        return HTMLResponse(f"<b>Stock file fetch failed:</b> {e}", status_code=500)

    stock_left = {k: stock_map[k].copy() for k in stock_map}
    supplier_orders = {'Nortons': {}, 'Nisbets': {}}
    stock_ship_orders = {}
    nisbets_shipped = set()
    nortons_shipped = set()

    for _, row in orders.iterrows():
        order_no = row['Order number']
        sku = row['Offer SKU']
        qty = int(row['Quantity'])
        supplier = row['Supplier Name']
        if supplier not in ['Nortons', 'Nisbets']:
            continue
        in_stock = stock_left[supplier].get(sku, 0)
        from_stock = min(qty, in_stock)
        to_supplier = qty - from_stock

        if from_stock > 0:
            if order_no not in stock_ship_orders:
                stock_ship_orders[order_no] = []
            stock_ship_orders[order_no].append((sku, from_stock))
            stock_left[supplier][sku] = max(in_stock - from_stock, 0)
            if supplier == 'Nisbets':
                nisbets_shipped.add(sku)
            else:
                nortons_shipped.add(sku)

        if to_supplier > 0:
            if order_no not in supplier_orders[supplier]:
                supplier_orders[supplier][order_no] = []
            supplier_orders[supplier][order_no].append((sku, to_supplier))

    def format_order_block(order_dict, title):
        out = []
        for order, lines in order_dict.items():
            out.append(f"Order Number: {order}\n")
            for sku, qty in lines:
                out.append(f"·        {qty}x {sku}\n")
            out.append("\n------------------------------\n\n")
        return "".join(out) if out else f"No {title.lower()}."

    nortons_out = format_order_block(supplier_orders['Nortons'], "Nortons orders")
    nisbets_out = format_order_block(supplier_orders['Nisbets'], "Nisbets orders")
    stock_out = format_order_block(stock_ship_orders, "stock shipments")

    # --- Build Nisbets.csv
    nisbets_csv_rows = []
    for order, lines in supplier_orders['Nisbets'].items():
        for sku, qty in lines:
            nisbets_csv_rows.append({'Order Number': order, 'Offer SKU': sku, 'Quantity': qty})
    if nisbets_csv_rows:
        nisbets_csv_df = pd.DataFrame(nisbets_csv_rows)
        csv_buffer = StringIO()
        nisbets_csv_df.to_csv(csv_buffer, index=False)
        request.app.state.latest_nisbets_csv = csv_buffer.getvalue().encode('utf-8')
        download_link = "<a href='/download_nisbets_csv' download='Nisbets.csv'><button class='copy-btn' style='right:auto;top:auto;position:relative;margin-bottom:1em;'>Download Nisbets CSV</button></a>"
    else:
        request.app.state.latest_nisbets_csv = None
        download_link = ""

    # --- Update stock DataFrames
    for sku in nisbets_shipped:
        if sku in nisbets_stock['Offer SKU'].values:
            idx = nisbets_stock[nisbets_stock['Offer SKU'] == sku].index[0]
            nisbets_stock.at[idx, 'Quantity'] = max(stock_left['Nisbets'].get(sku, 0), 0)
    for sku in nortons_shipped:
        if sku in nortons_stock['Offer SKU'].values:
            idx = nortons_stock[nortons_stock['Offer SKU'] == sku].index[0]
            nortons_stock.at[idx, 'Quantity'] = max(stock_left['Nortons'].get(sku, 0), 0)
    try:
        if nisbets_shipped:
            upload_excel_file(NISBETS_STOCK_FILE_ID, nisbets_stock)
        if nortons_shipped:
            upload_excel_file(NORTONS_STOCK_FILE_ID, nortons_stock)
    except Exception as e:
        if "423" in str(e):
            return HTMLResponse("<b>Stock file update failed: File is open or locked in Excel.<br>Please close the file everywhere and try again in a minute.</b>", status_code=423)
        return HTMLResponse(f"<b>Stock file update failed:</b> {e}", status_code=500)

    # --- Update OrderHistory.csv (append) ---
    try:
        old_hist = download_csv_file(ORDER_HISTORY_FILE_ID)
        new_hist = pd.concat([old_hist, df], ignore_index=True)
        upload_csv_file(ORDER_HISTORY_FILE_ID, new_hist)
    except Exception:
        upload_csv_file(ORDER_HISTORY_FILE_ID, df)

    html = f"""
    <style>
    .out-card {{ background:#f7fafc; border-radius:10px; margin:1.5em 0; padding:1.3em 1.5em; box-shadow:0 2px 8px #0001; position:relative;}}
    .copy-btn {{ position:absolute; right:24px; top:26px; background:#3b82f6; color:#fff; border:none; border-radius:4px; padding:5px 15px; cursor:pointer; font-size:1em;}}
    .copy-btn:hover {{ background:#2563eb; }}
    h3 {{ margin-top:0; }}
    pre {{ white-space: pre-wrap; font-family:inherit; font-size:1.09em; margin:0;}}
    </style>
    <div class="out-card">
      <h3>Nortons (Order from Supplier)</h3>
      <button class="copy-btn" onclick="navigator.clipboard.writeText(document.getElementById('nortonsout').innerText)">Copy</button>
      <pre id="nortonsout">{nortons_out}</pre>
    </div>
    <div class="out-card">
      <h3>Nisbets (Order from Supplier)</h3>
      {download_link}
      <button class="copy-btn" onclick="navigator.clipboard.writeText(document.getElementById('nisbetsout').innerText)">Copy</button>
      <pre id="nisbetsout">{nisbets_out}</pre>
    </div>
    <div class="out-card">
      <h3>Ship from Stock</h3>
      <button class="copy-btn" onclick="navigator.clipboard.writeText(document.getElementById('stockout').innerText)">Copy</button>
      <pre id="stockout">{stock_out}</pre>
    </div>
    <div style='margin-top:2em;text-align:center;'>
        {download_link}
    </div>
    """
    return HTMLResponse(html)

@app.get("/download_nisbets_csv")
async def download_nisbets_csv(request: Request):
    data = getattr(request.app.state, 'latest_nisbets_csv', None)
    if not data:
        return HTMLResponse("<b>No Nisbets CSV generated in this session yet.</b>", status_code=404)
    return StreamingResponse(
        BytesIO(data),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=Nisbets.csv"}
    )

# ========== END SECTION 3 ==========
# ========== SECTION 4: Utility Endpoints & Downloads ==========

# Helper: Download OrderHistory as CSV (for backup or admin)
@app.get("/admin/download_orderhistory")
def download_orderhistory():
    try:
        df = download_csv_file(ORDER_HISTORY_FILE_ID)
    except Exception:
        return HTMLResponse("<b>No order history found.</b>", status_code=404)
    buffer = StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    return StreamingResponse(buffer, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=OrderHistory.csv"})

# Download supplier order file by supplier name (xlsx)
@app.get("/supplier_orders/{supplier}.xlsx")
def supplier_order_xlsx(supplier: str):
    try:
        orders = download_csv_file(ORDER_HISTORY_FILE_ID)
    except Exception:
        return HTMLResponse("<b>No OrderHistory found.</b>", status_code=404)
    orders = orders[orders['Supplier Name'].str.lower() == supplier.lower()]
    if orders.empty:
        return HTMLResponse(f"<b>No orders for supplier {supplier}.</b>", status_code=404)
    output = BytesIO()
    orders[['Order number', 'Offer SKU', 'Quantity']].to_excel(output, index=False)
    output.seek(0)
    return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            headers={"Content-Disposition": f"attachment; filename={supplier}_Order_List.xlsx"})

# Utility: Download DPD label file if available (optional)
@app.get("/download_dpd_csv")
async def download_dpd_csv(request: Request):
    data = getattr(request.app.state, 'latest_dpd_csv', None)
    if not data:
        return HTMLResponse("<b>No DPD CSV generated in this session yet.</b>", status_code=404)
    return StreamingResponse(
        BytesIO(data),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=DPD_Export.csv"}
    )

# Utility: Download Zoho file if available (optional)
@app.get("/download_zoho_xlsx")
async def download_zoho_xlsx(request: Request):
    data = getattr(request.app.state, 'latest_zoho_xlsx', None)
    if not data:
        return HTMLResponse("<b>No Zoho XLSX generated in this session yet.</b>", status_code=404)
    return StreamingResponse(
        BytesIO(data),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=zoho_orders.xlsx"}
    )

# ========== END OF FILE ==========

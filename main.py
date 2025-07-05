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
UPLOAD_LOG_FILE_ID=01YTGSV5GJJRXXXWMWPRHKYWSK4K4P3WLC


# --- App & Templates ---
app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="ultra-secret-CHANGE-THIS")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# --- Helpers ---
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

# --- Admin logic ---
ADMIN_PASSWORD = "Orendaent101!"

def is_admin(request: Request):
    return request.session.get("admin_logged_in", False)

@app.get("/admin/login")
def admin_login_page(request: Request):
    return templates.TemplateResponse("admin.html", {"request": request, "error": None})

@app.post("/admin/login")
def admin_login(request: Request, password: str = Form(...)):
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

# --- Main upload/order logic (single upload form) ---
@app.get("/", response_class=HTMLResponse)
def main_upload_form(request: Request):
    return templates.TemplateResponse("main_upload.html", {"request": request})

@app.post("/upload_orders/display")
async def upload_orders_display(request: Request, file: UploadFile = File(...)):
    try:
        df = pd.read_excel(file.file) if file.filename.lower().endswith("xlsx") else pd.read_csv(file.file)
        if not set(['Order number', 'Offer SKU', 'Quantity']).issubset(df.columns):
            raise Exception("Missing columns: must have 'Order number', 'Offer SKU', 'Quantity'")
        orders = df[['Order number', 'Offer SKU', 'Quantity']].dropna()
    except Exception as e:
        return HTMLResponse(f"<b>Order file read failed or missing columns:</b> {e}", status_code=500)

    try:
        supplier_df = download_csv_file(SUPPLIER_FILE_ID)
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

    orders['Need To Order'] = orders.apply(
        lambda x: max(0, x['Quantity'] - stock_map.get(x['Supplier Name'], {}).get(x['Offer SKU'], 0)),
        axis=1
    )
    orders = orders[orders['Need To Order'] > 0]

    # --- Split for each supplier and prep files ---
    for supplier in ['Nisbets', 'Nortons']:
        supplier_orders = orders[orders['Supplier Name'] == supplier]
        if not supplier_orders.empty:
            filename = f"{supplier}_Order_List.xlsx"
            output = BytesIO()
            supplier_orders[['Order number', 'Offer SKU', 'Quantity']].to_excel(output, index=False)
            output.seek(0)
            # You can add download endpoints or save for later download as needed

    # --- DPD label logic: group by Order number, count parcels
    orders['Parcel Count'] = 1  # Default to 1 unless overridden
    dpd_labels = orders.groupby('Order number').agg({
        'Parcel Count': 'sum',
        'Customer Name': 'first'
    }).reset_index()
    # Add CSV download if needed

    # --- Update OrderHistory ---
    try:
        old_hist = download_csv_file(ORDER_HISTORY_FILE_ID)
        new_hist = pd.concat([old_hist, df], ignore_index=True)
        upload_csv_file(ORDER_HISTORY_FILE_ID, new_hist)
    except Exception:
        upload_csv_file(ORDER_HISTORY_FILE_ID, df)

    return HTMLResponse("<b>Order processed and added to OrderHistory.csv!</b>")

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

# --- Supplier Order Download (on demand for latest batch) ---
@app.get("/supplier_orders/{supplier}.xlsx")
def supplier_order_xlsx(supplier: str):
    orders = download_csv_file(ORDER_HISTORY_FILE_ID)
    supplier_orders = orders[orders['Supplier Name'].str.lower() == supplier.lower()]
    output = BytesIO()
    supplier_orders[['Order number', 'Offer SKU', 'Quantity']].to_excel(output, index=False)
    output.seek(0)
    return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={
        "Content-Disposition": f"attachment; filename={supplier}_Order_List.xlsx"
    })

# --- You can add more endpoints as needed for other business logic ---

# --------------- END OF FILE ---------------

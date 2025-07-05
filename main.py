import os
import requests
import pandas as pd
from fastapi import FastAPI, File, UploadFile, Form, Request, Depends
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from io import BytesIO, StringIO
import json
from datetime import datetime
from typing import Optional

# --- ENV / CONSTANTS ---
TENANT_ID = os.getenv("TENANT_ID", "ce280aae-ee92-41fe-ab60-66b37ebc97dd")
CLIENT_ID = os.getenv("CLIENT_ID", "83acd574-ab02-4cfe-b28c-e38c733d9a52")
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "FYX8Q~bZVXuKEenMTryxYw-ZuQOq2OBTNIu8Qa~i")
DRIVE_ID = os.getenv("DRIVE_ID", "b!udRZ7OsrmU61CSAYEn--q1fPtuPR3TZAsv2B9cCW-gzWb8B-lsUaQLURaNYNJxjP")
SUPPLIER_FILE_ID = os.getenv("SUPPLIER_FILE_ID", "01YTGSV5DGZEMEISWEYVDJRULO4ADDVCVQ")
NISBETS_STOCK_FILE_ID = os.getenv("NISBETS_STOCK_FILE_ID", "01YTGSV5GERF436HITURGITCR3M7XMYJHF")
NORTONS_STOCK_FILE_ID = os.getenv("NORTONS_STOCK_FILE_ID", "01YTGSV5FKHUI4S6BVWJDLNWETK4TUU26D")
SKU_MAX_FILE_ID = os.getenv("SKU_MAX_FILE_ID", "01YTGSV5DOW27RMJGS3JA2IODH6HCF4647")
ORDER_HISTORY_FILE_ID = os.getenv("ORDER_HISTORY_FILE_ID", "01YTGSV5BZ2T4AVNGCU5F3EWLPXYMKFATG")

ADMIN_PASSWORD = "Orendaent101!"

# --- FASTAPI / STATIC / TEMPLATES ---
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# --- HELPERS ---
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

def download_onedrive_file(file_id, filetype='csv'):
    token = get_graph_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{file_id}/content"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    if filetype == 'csv':
        return pd.read_csv(BytesIO(r.content))
    elif filetype == 'xlsx':
        return pd.read_excel(BytesIO(r.content))
    else:
        return r.content

def upload_onedrive_file(file_id, df, filetype='csv'):
    token = get_graph_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/vnd.ms-excel" if filetype == 'csv' else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    }
    buffer = BytesIO()
    if filetype == 'csv':
        df.to_csv(buffer, index=False)
    else:
        df.to_excel(buffer, index=False)
    buffer.seek(0)
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{file_id}/content"
    r = requests.put(url, headers=headers, data=buffer.read())
    r.raise_for_status()
    return True

def load_sku_max_map():
    # Download and parse JSON from OneDrive
    try:
        raw = download_onedrive_file(SKU_MAX_FILE_ID, filetype='json')
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return {}

def save_sku_max_map(data):
    token = get_graph_access_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{SKU_MAX_FILE_ID}/content"
    r = requests.put(url, headers=headers, data=json.dumps(data).encode('utf-8'))
    r.raise_for_status()
    return True

def append_order_history(df):
    try:
        existing = download_onedrive_file(ORDER_HISTORY_FILE_ID, 'csv')
        all_df = pd.concat([existing, df], ignore_index=True)
        upload_onedrive_file(ORDER_HISTORY_FILE_ID, all_df, 'csv')
    except Exception:
        upload_onedrive_file(ORDER_HISTORY_FILE_ID, df, 'csv')

def admin_auth(request: Request):
    session_password = request.cookies.get("admin_auth", None)
    return session_password == ADMIN_PASSWORD

def set_admin_cookie(response: Response):
    response.set_cookie("admin_auth", ADMIN_PASSWORD, httponly=True, max_age=60*60*24*10)

# --- ROUTES ---

@app.get("/", response_class=HTMLResponse)
async def homepage(request: Request):
    return """
    <div style='text-align:center;margin:4em auto;max-width:400px;padding:3em 2em;background:#fff;border-radius:16px;box-shadow:0 3px 32px #0001;'>
        <img src='/static/logo.png' style='max-width:230px;margin-bottom:2em'>
        <h2>Order Upload</h2>
        <form id='uploadForm' enctype='multipart/form-data' style='margin-bottom:2em'>
            <input name='file' type='file' accept='.xlsx,.csv' required style='margin-bottom:1.5em'><br>
            <button type='submit'>Upload Orders File</button>
        </form>
        <div id='results'></div>
        <button onclick='window.location=\"/admin\"' style='margin-top:2em;background:#5562fd;color:#fff;border:none;border-radius:6px;padding:0.6em 1.6em;cursor:pointer;'>Admin Settings</button>
        <button onclick='window.location=\"/upload_history\"' style='margin-top:2em;background:#27b6a4;color:#fff;border:none;border-radius:6px;padding:0.6em 1.6em;cursor:pointer;'>Upload History</button>
        <button onclick='window.location=\"/dashboard\"' style='margin-top:2em;background:#FFAC2A;color:#fff;border:none;border-radius:6px;padding:0.6em 1.6em;cursor:pointer;'>Dashboard</button>
        <div class="footer" style="margin-top:2em;font-size:0.92em;color:#777;">Caterboss Orders &copy; 2025</div>
        <script>
            document.getElementById('uploadForm').onsubmit = async function(e){
                e.preventDefault();
                let formData = new FormData(this);
                document.getElementById('results').innerHTML = "<em>Uploading & processing...</em>";
                let res = await fetch('/upload_orders', { method: 'POST', body: formData });
                let html = await res.text();
                document.getElementById('results').innerHTML = html;
            };
        </script>
    </div>
    """

@app.post("/upload_orders", response_class=HTMLResponse)
async def upload_orders(request: Request, file: UploadFile = File(...)):
    # Read file as dataframe (csv or xlsx)
    ext = os.path.splitext(file.filename)[1].lower()
    if ext == ".csv":
        df = pd.read_csv(file.file)
    else:
        df = pd.read_excel(file.file)
    # Append order history
    append_order_history(df)
    # Log upload event (can be stored in a separate csv if you want)
    # ... (not implemented yet)
    return f"""
    <div style='padding:1.2em;'>
        <b>File uploaded and processed successfully!</b><br>
        <a href="/">Back</a>
    </div>
    """

@app.get("/admin", response_class=HTMLResponse)
async def admin_login(request: Request):
    if admin_auth(request):
        return RedirectResponse("/admin/home")
    return """
    <div style='margin:7em auto;max-width:350px;background:#fff;padding:2em 2em 2.6em 2em;border-radius:13px;box-shadow:0 2px 18px #0001;text-align:center;'>
        <img src='/static/logo.png' style='max-width:140px;margin-bottom:1.7em'>
        <form method='post' action='/admin/login'>
            <input name='password' type='password' placeholder='Admin password' style='margin-bottom:1.4em;width:80%;padding:0.7em;border-radius:7px;'><br>
            <button type='submit'>Login</button>
        </form>
        <div style='margin-top:1.2em'><a href="/">Back</a></div>
    </div>
    """

@app.post("/admin/login", response_class=HTMLResponse)
async def admin_login_post(request: Request):
    form = await request.form()
    if form.get("password") == ADMIN_PASSWORD:
        resp = RedirectResponse(url="/admin/home", status_code=302)
        set_admin_cookie(resp)
        return resp
    return "<b>Incorrect password.</b> <a href='/admin'>Back</a>"

@app.get("/admin/home", response_class=HTMLResponse)
async def admin_home(request: Request):
    if not admin_auth(request):
        return RedirectResponse("/admin")
    # Display undo and SKU max settings links
    return """
    <div style='margin:4em auto;max-width:460px;padding:2.5em 2em;background:#fff;border-radius:12px;box-shadow:0 3px 22px #0001;'>
        <img src='/static/logo.png' style='max-width:120px;margin-bottom:1.7em'>
        <h2>Admin Settings</h2>
        <button onclick='window.location=\"/admin/undo\"' style='background:#e53935;color:#fff;border:none;border-radius:6px;padding:0.7em 1.6em;margin-bottom:1.2em;cursor:pointer;'>Undo Last Stock Update</button><br>
        <button onclick='window.location=\"/admin/skumax\"' style='background:#3b82f6;color:#fff;border:none;border-radius:6px;padding:0.7em 1.6em;margin-bottom:1.2em;cursor:pointer;'>Set Max Per Parcel</button><br>
        <button onclick='window.location=\"/dashboard\"' style='background:#FFAC2A;color:#fff;border:none;border-radius:6px;padding:0.7em 1.6em;cursor:pointer;'>View Dashboard</button><br>
        <a href="/">Back to Home</a>
    </div>
    """

@app.get("/admin/undo", response_class=HTMLResponse)
async def admin_undo(request: Request):
    if not admin_auth(request):
        return RedirectResponse("/admin")
    # Restore previous version code...
    # (put your undo logic here)
    return """
    <div style='margin:4em auto;max-width:430px;padding:2.1em;background:#fff;border-radius:11px;box-shadow:0 3px 22px #0001;text-align:center'>
        <b>Restore Stock Files (prev version)</b><br>
        <form method='post' action='/admin/undo/submit' style='margin-top:2em'>
            <button type='submit' style='background:#e53935;color:#fff;border:none;border-radius:6px;padding:0.6em 1.7em;cursor:pointer;'>Restore Now</button>
        </form>
        <div style='margin-top:2em'><a href='/admin/home'>Back</a></div>
    </div>
    """

@app.post("/admin/undo/submit", response_class=HTMLResponse)
async def admin_undo_submit(request: Request):
    if not admin_auth(request):
        return RedirectResponse("/admin")
    # TODO: Implement OneDrive file version restore logic
    return "<b>Stock files restored to previous version (mocked)</b> <a href='/admin/home'>Back</a>"

@app.get("/admin/skumax", response_class=HTMLResponse)
async def admin_skumax(request: Request):
    if not admin_auth(request):
        return RedirectResponse("/admin")
    sku_map = load_sku_max_map()
    rules_html = "<ul>" + "".join([f"<li><b>{sku}</b>: {qty}</li>" for sku, qty in sku_map.items()]) + "</ul>" if sku_map else "<i>No max per parcel rules set.</i>"
    return f"""
    <div style='margin:4em auto;max-width:440px;padding:2.3em;background:#fff;border-radius:11px;box-shadow:0 3px 22px #0001;text-align:center'>
        <b>Set Max Per Parcel</b>
        <form method='post' action='/admin/skumax/submit' style='margin-top:1.3em;margin-bottom:1.5em;'>
            <input name='sku' required placeholder='SKU' style='margin-right:0.7em;'>
            <input name='max_qty' type='number' min='1' required placeholder='Max per parcel' style='width:60px;'>
            <button type='submit'>Set</button>
        </form>
        <div>{rules_html}</div>
        <div style='margin-top:2em'><a href='/admin/home'>Back</a></div>
    </div>
    """

@app.post("/admin/skumax/submit", response_class=HTMLResponse)
async def admin_skumax_submit(request: Request):
    if not admin_auth(request):
        return RedirectResponse("/admin")
    form = await request.form()
    sku = form.get("sku", "").strip()
    max_qty = int(form.get("max_qty", "1"))
    sku_map = load_sku_max_map()
    sku_map[sku] = max_qty
    save_sku_max_map(sku_map)
    return RedirectResponse("/admin/skumax", status_code=302)

@app.get("/upload_history", response_class=HTMLResponse)
async def upload_history(request: Request):
    # Display list of filenames and times of all order uploads
    # To be implemented: for now, placeholder
    return """
    <div style='margin:4em auto;max-width:500px;padding:2em;background:#fff;border-radius:11px;box-shadow:0 3px 22px #0001;'>
        <h2>Upload History</h2>
        <i>Feature under construction.</i>
        <div style='margin-top:2em'><a href="/">Back</a></div>
    </div>
    """

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request, top: Optional[int]=10):
    # Read OrderHistory.csv from OneDrive
    try:
        df = download_onedrive_file(ORDER_HISTORY_FILE_ID, filetype='csv')
    except Exception as e:
        return f"<b>Could not load OrderHistory.csv: {e}</b>"
    # Calculate dashboard stats
    df['Order number'] = df['Order number'].astype(str)
    sku_stats = df.groupby('Offer SKU')['Quantity'].sum().sort_values(ascending=False)
    customer_stats = df.groupby('Customer Name')['Quantity'].sum().sort_values(ascending=False)
    total_orders = df['Order number'].nunique()
    total_sales = df['Quantity'].sum()
    # ... more metrics here ...
    top_skus = sku_stats.head(top).reset_index()
    top_customers = customer_stats.head(top).reset_index()
    # Render
    sku_rows = "".join([f"<tr><td>{row['Offer SKU']}</td><td>{row['Quantity']}</td></tr>" for _, row in top_skus.iterrows()])
    customer_rows = "".join([f"<tr><td>{row['Customer Name']}</td><td>{row['Quantity']}</td></tr>" for _, row in top_customers.iterrows()])
    return f"""
    <div style='margin:4em auto;max-width:920px;padding:2.3em;background:#fff;border-radius:16px;box-shadow:0 3px 32px #0001;'>
        <h2>Dashboard</h2>
        <b>Total Orders:</b> {total_orders}<br>
        <b>Total Sales (Qty):</b> {total_sales}
        <div style='margin:2em 0;display:flex;gap:3em;'>
            <div>
                <h3>Top {top} SKUs</h3>
                <table style='border-collapse:collapse;'><tr><th>SKU</th><th>Qty</th></tr>
                {sku_rows}
                </table>
            </div>
            <div>
                <h3>Top {top} Customers</h3>
                <table style='border-collapse:collapse;'><tr><th>Customer</th><th>Qty</th></tr>
                {customer_rows}
                </table>
            </div>
        </div>
        <a href='/dashboard?top=10'>Top 10</a> | <a href='/dashboard?top=50'>Top 50</a> | <a href='/dashboard?top=100'>Top 100</a> | <a href='/dashboard?top=99999'>All</a><br>
        <div style='margin-top:2em'><a href="/">Back</a></div>
    </div>
    """

# --- (end of file) ---


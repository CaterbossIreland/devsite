import os
import requests
import pandas as pd
from fastapi.responses import FileResponse
import tempfile
from fastapi import FastAPI, File, UploadFile, Request, Form
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
from starlette.middleware.sessions import SessionMiddleware
from fastapi.staticfiles import StaticFiles
from io import BytesIO, StringIO
from datetime import datetime
import json

# ---------- CONFIGURATION ----------
TENANT_ID = os.getenv("TENANT_ID", "ce280aae-ee92-41fe-ab60-66b37ebc97dd")
CLIENT_ID = os.getenv("CLIENT_ID", "83acd574-ab02-4cfe-b28c-e38c733d9a52")
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "FYX8Q~bZVXuKEenMTryxYw-ZuQOq2OBTNIu8Qa~i")
DRIVE_ID = os.getenv("DRIVE_ID", "b!udRZ7OsrmU61CSAYEn--q1fPtuPR3TZAsv2B9cCW-gzWb8B-lsUaQLURaNYNJxjP")
SUPPLIER_FILE_ID = os.getenv("SUPPLIER_FILE_ID", "01YTGSV5DGZEMEISWEYVDJRULO4ADDVCVQ")
NISBETS_STOCK_FILE_ID = os.getenv("NISBETS_STOCK_FILE_ID", "01YTGSV5HJCNBDXINJP5FJE2TICQ6Q3NEX")
NORTONS_STOCK_FILE_ID = os.getenv("NORTONS_STOCK_FILE_ID", "01YTGSV5FBVS7JYODGLREKL273FSJ3XRLP")
SKU_MAX_FILE_ID = os.getenv("SKU_MAX_FILE_ID", "01YTGSV5DOW27RMJGS3JA2IODH6HCF4647")
PO_MAP_FILE_ID = os.getenv("PO_MAP_FILE_ID", "01YTGSV5D4WTSUTV3D7FGKT6YKUKV4BIYI")
ZOHO_TEMPLATE_PATH = "column format.xlsx"
DPD_TEMPLATE_PATH = "DPD.Import(1).csv"

def upload_po_map(po_map):
    token = get_graph_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    data = json.dumps(po_map).encode("utf-8")
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{PO_MAP_FILE_ID}/content"
    r = requests.put(url, headers=headers, data=data)
    r.raise_for_status()

def download_po_map():
    token = get_graph_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{PO_MAP_FILE_ID}/content"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return json.loads(r.content.decode())

def save_po_map(po_number, batch_rows):
    try:
        po_map = download_po_map()
    except Exception:
        po_map = {}
    po_map[po_number] = batch_rows
    upload_po_map(po_map)



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

def download_excel_file(file_id):
    token = get_graph_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{file_id}/content"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return pd.read_excel(BytesIO(r.content))

def download_supplier_csv():
    token = get_graph_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{SUPPLIER_FILE_ID}/content"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return pd.read_csv(BytesIO(r.content))

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

def get_dpd_template_columns(template_path):
    with open(template_path, "r", encoding="utf-8") as f:
        sample = f.read(2048)
    delimiter = "," if sample.count(",") > sample.count(";") else ";"
    df = pd.read_csv(template_path, header=None, delimiter=delimiter)
    headers = list(df.iloc[1])
    return df, headers, delimiter

def load_sku_limits():
    try:
        token = get_graph_access_token()
        headers = {"Authorization": f"Bearer {token}"}
        url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{SKU_MAX_FILE_ID}/content"
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        return json.loads(resp.content.decode())
    except Exception as e:
        print(f"Error loading SKU max per parcel from OneDrive: {e}")
        return {}

def save_sku_limits(limits):
    try:
        token = get_graph_access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        data = json.dumps(limits).encode("utf-8")
        url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{SKU_MAX_FILE_ID}/content"
        resp = requests.put(url, headers=headers, data=data)
        resp.raise_for_status()
        return True
    except Exception as e:
        print(f"Error saving SKU max per parcel to OneDrive: {e}")
        return False

def get_previous_version_id(file_id):
    token = get_graph_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{file_id}/versions"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    versions = resp.json().get("value", [])
    if len(versions) < 2:
        return None
    return versions[1]['id']

def restore_file_version(file_id, version_id):
    token = get_graph_access_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{file_id}/versions/{version_id}/restoreVersion"
    resp = requests.post(url, headers=headers)
    resp.raise_for_status()
    return resp.ok

# --- PO Mapping helpers ---

def generate_po_number(batch_idx):
    today = datetime.now().strftime("%Y%m%d")
    return f"CB-NISBETS-{today}-{batch_idx+1:03d}"

def save_po_map(po_number, batch_rows):
    try:
        po_map = download_po_map()
    except Exception:
        po_map = {}
    po_map[po_number] = batch_rows
    upload_po_map(po_map)
    

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="!supersecret!")
app.mount("/static", StaticFiles(directory="static"), name="static")

latest_nisbets_csv_batches = {}
latest_zoho_xlsx = None
latest_dpd_csv = None
dpd_error_report_html = ""
from fastapi import Request
@app.get("/admin-login", response_class=HTMLResponse)
async def login_form(request: Request):
    return """
    <style>
    body { background: #f3f6f9; }
    .login-container {
        max-width: 400px; margin: 8em auto; background: #fff; border-radius: 12px;
        box-shadow: 0 2px 12px #0002; padding: 2em; font-family: 'Segoe UI',Arial,sans-serif;
    }
    </style>
    <div class="login-container">
        <h2>Admin Login</h2>
        <form action="/admin-login" method="post">
            <input type="password" name="password" placeholder="Enter admin password" style="width:100%;margin-bottom:1em;">
            <button type="submit" style="width:100%;">Login</button>
        </form>
    </div>
    """

@app.post("/admin-login")
async def login_submit(request: Request, password: str = Form(...)):
    # Hardcoded password for demonstration. Replace with your logic!
    if password == "caterboss2025":
        request.session["admin_logged_in"] = True
        return RedirectResponse("/admin", status_code=303)
    else:
        return HTMLResponse(
            "<b style='color:red'>Incorrect password. <a href='/admin-login'>Try again</a></b>", status_code=401
        )
@app.get("/admin", response_class=HTMLResponse)
async def admin_dashboard(request: Request):
    if not request.session.get("admin_logged_in"):
        return RedirectResponse("/admin-login", status_code=303)
    # Load SKU limits for listing
    sku_limits = load_sku_limits()
    sku_list_html = "".join(
        f"<tr><td>{sku}</td><td>{max_per}</td></tr>"
        for sku, max_per in sku_limits.items()
    )
    return f"""
    <style>
    .admin-container {{
        max-width: 700px;
        margin: 3em auto;
        background: #fff;
        border-radius: 14px;
        box-shadow: 0 2px 16px #0001;
        padding: 2.5em;
        font-family: 'Segoe UI',Arial,sans-serif;
    }}
    table {{
        border-collapse: collapse;
        width: 100%;
    }}
    th, td {{
        padding: 0.5em;
        border-bottom: 1px solid #eee;
        text-align: left;
    }}
    </style>
    <div class="admin-container">
        <h2>Admin Dashboard</h2>
        <div style="margin-bottom:2em;">
            <a href="/"><button style="background:#3b82f6;color:#fff;border:none;border-radius:6px;padding:0.5em 1.3em;font-size:1em;">Upload Orders File</button></a>
        </div>
        <form method="post" action="/admin/set-max-sku">
            <h3>Set Max Per Parcel for SKU</h3>
            <input type="text" name="sku" placeholder="SKU" required>
            <input type="number" name="max_per_parcel" placeholder="Max Per Parcel" required>
            <button type="submit">Set</button>
        </form>
        <form method="post" action="/admin/delete-max-sku" style="margin-top:1em;">
            <h3>Delete Max Per Parcel Rule</h3>
            <input type="text" name="sku" placeholder="SKU" required>
            <button type="submit">Delete</button>
        </form>
        <form method="post" action="/admin/undo-stock-update" style="margin-top:1em;">
            <button type="submit">Undo Last Stock Update (Nisbets & Nortons)</button>
        </form>
        <div style="margin-top:2em;">
            <a href="/admin/musgraves-dpd-upload">Musgraves DPD Upload Tool →</a>
        </div>
        <div style="margin-top:2em;">
            <h3>Current SKU Max Limits</h3>
            <table>
                <tr><th>SKU</th><th>Max Per Parcel</th></tr>
                {sku_list_html}
            </table>
        </div>
        <div style="margin-top:2em;">
            <form method="post" action="/logout">
                <button type="submit">Logout</button>
            </form>
            <div style="margin-top:2em;">
    <a href="/admin/lookup">PO & SKU Lookup Tool →</a>
</div>

        </div>
    </div>
    """


@app.get("/", response_class=HTMLResponse)
async def main_upload_form(request: Request):
    if not request.session.get("admin_logged_in"):
        return RedirectResponse("/admin-login", status_code=303)
    return """
    <style>
    body { font-family: 'Segoe UI',Arial,sans-serif; background: #f3f6f9; margin: 0; padding: 0;}
    .container { max-width: 720px; margin: 3em auto; background: #fff; border-radius: 14px; box-shadow: 0 2px 16px #0001; padding: 2.5em;}
    .logo-wrap {text-align: center; margin-top: 0; margin-bottom: 2em;}
    .logo-img {max-width: 250px; height: auto;}
    h2 { margin-bottom: 0.5em; }
    .upload-form { display: flex; flex-direction: column; gap: 1em;}
    button { background: #3b82f6; color: #fff; border: none; border-radius: 6px; font-size: 1.1em; padding: 0.7em 2em; cursor: pointer;}
    button:hover { background: #2563eb; }
    .footer { margin-top: 2em; text-align: center; color: #888;}
    </style>
    <div style="text-align:right;">
      <a href="/admin-login"><button style="background:#3b82f6;color:#fff;border:none;border-radius:6px;padding:0.5em 1.3em;font-size:1em;">Admin Login</button></a>
    </div>
    <div class="container">
      <div class="logo-wrap">
        <img src="/static/logo.png" alt="Logo" class="logo-img" />
      </div>
      <h2>Upload Orders File</h2>
      <form class="upload-form" id="uploadForm" enctype="multipart/form-data">
        <input name="file" type="file" accept=".xlsx" required>
        <button type="submit">Upload & Show Output</button>
      </form>
      <div id="results"></div>
    </div>
    <div class="footer">Caterboss Orders &copy; 2025</div>
    <script>
    document.getElementById('uploadForm').onsubmit = async function(e){
      e.preventDefault();
      let formData = new FormData(this);
      document.getElementById('results').innerHTML = "<em>Processing...</em>";
      let res = await fetch('/upload_orders/display', { method: 'POST', body: formData });
      let html = await res.text();
      document.getElementById('results').innerHTML = html;
      window.scrollTo(0,document.body.scrollHeight);
    }
    </script>
    """

@app.get("/download_zoho_xlsx")
async def download_zoho_xlsx():
    if not latest_zoho_xlsx:
        return HTMLResponse("<b>No Zoho XLSX generated in this session yet.</b>", status_code=404)
    return StreamingResponse(
        BytesIO(latest_zoho_xlsx),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=zoho_orders.xlsx"}
    )

@app.get("/download_dpd_csv")
async def download_dpd_csv():
    if not latest_dpd_csv:
        return HTMLResponse("<b>No DPD CSV generated in this session yet.</b>", status_code=404)
    return StreamingResponse(
        BytesIO(latest_dpd_csv),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=DPD_Export.csv"}
    )
from fastapi import Request

@app.post("/upload_orders/display")
async def upload_orders_display(request: Request, file: UploadFile = File(...)):
    # Require admin login!
    if not request.session.get("admin_logged_in"):
        return RedirectResponse("/admin-login", status_code=303)

    global latest_nisbets_csv_batches, latest_zoho_xlsx, latest_dpd_csv, dpd_error_report_html
    latest_nisbets_csv_batches = {}  # Reset batches

    # --- 1. Read Orders File ---
    try:
        df = pd.read_excel(file.file)
        orders = df[['Order number', 'Offer SKU', 'Quantity']].dropna()
    except Exception as e:
        return HTMLResponse(f"<b>Order file read failed or missing columns:</b> {e}", status_code=500)

    # --- 2. Supplier Map & unmatched detection ---
    try:
        supplier_df = download_supplier_csv()
        sku_to_supplier = dict(zip(supplier_df['Offer SKU'], supplier_df['Supplier Name']))
        orders['Supplier Name'] = orders['Offer SKU'].map(sku_to_supplier)
        # Find unmatched SKUs
        unmatched = orders[orders['Supplier Name'].isna()][['Order number', 'Offer SKU', 'Quantity']]
        unmatched_report = []
        for _, row in unmatched.iterrows():
            unmatched_report.append({
                'order_no': row['Order number'],
                'sku': row['Offer SKU'],
                'qty': int(row['Quantity'])
            })
    except Exception as e:
        return HTMLResponse(f"<b>Supplier fetch/mapping failed:</b> {e}", status_code=500)

    # --- 3. Stock ---
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

    # --- 4. Nisbets batch splitting ---
    nisbets_orders = list(supplier_orders['Nisbets'].keys())
    max_orders_per_file = 20
    nisbets_batches = [
        nisbets_orders[i:i+max_orders_per_file]
        for i in range(0, len(nisbets_orders), max_orders_per_file)
    ]
nisbets_csv_links = []
for idx, batch in enumerate(nisbets_batches):
    batch_rows = []
    for order in batch:
        for sku, qty in supplier_orders['Nisbets'][order]:
            batch_rows.append({'Order Number': order, 'Offer SKU': sku, 'Quantity': qty})
    if batch_rows:
        po_number = generate_po_number(idx)
        df_batch = pd.DataFrame(batch_rows)
        csv_buffer = StringIO()
        df_batch.to_csv(csv_buffer, index=False)
        csv_bytes = csv_buffer.getvalue().encode('utf-8')
        latest_nisbets_csv_batches[po_number] = csv_bytes  # KEY IS NOW PO NUMBER, not idx!
        save_po_map(po_number, batch_rows)
        link = f"<a href='/download_nisbets_csv/{po_number}' download='Nisbets_{po_number}.csv'><button class='copy-btn' style='right:auto;top:auto;position:relative;margin-bottom:1em;'>Download Nisbets CSV {po_number}</button></a>"
        nisbets_csv_links.append(link)

    download_link = "<br>".join(nisbets_csv_links) if nisbets_csv_links else ""

    def format_order_block(order_dict, title):
        out = []
        for order, lines in order_dict.items():
            out.append(f"Order Number: {order}\n")
            for sku, qty in lines:
                out.append(f"·        {qty}x {sku}\n")
            out.append("\n------------------------------\n\n")
        return "".join(out) if out else f"No {title.lower()}."

    nortons_out = format_order_block(supplier_orders['Nortons'], "Nortons orders")
    nisbets_batch_blocks = []
    for idx, batch in enumerate(nisbets_batches):
        batch_orders = {order: supplier_orders['Nisbets'][order] for order in batch}
        orders_text = format_order_block(batch_orders, f"Nisbets orders (Batch {idx+1})")
        download_btn = nisbets_csv_links[idx]
        nisbets_batch_blocks.append(f"""
        <div class="out-card">
          <h3>Nisbets Orders – Batch {idx+1}</h3>
          {download_btn}
          <button class="copy-btn" onclick="navigator.clipboard.writeText(document.getElementById('nisbetsout_{idx}').innerText)">Copy</button>
          <pre id="nisbetsout_{idx}">{orders_text}</pre>
        </div>
        """)
    nisbets_out = "\n".join(nisbets_batch_blocks)
    stock_out   = format_order_block(stock_ship_orders, "stock shipments")

    # --- 6. Zoho XLSX Generation ---
    try:
        template_df    = pd.read_excel(ZOHO_TEMPLATE_PATH)
        zoho_col_order = list(template_df.columns)
    except Exception as e:
        return HTMLResponse(
            f"<b>Failed to load Zoho template: {e}</b>",
            status_code=500
        )

    zoho_df = df.copy()
    if 'Date created' in zoho_df.columns:
        zoho_df['Date created'] = zoho_df['Date created'].astype(str).str.split().str[0]
    zoho_df['Shipping total amount'] = 4.95
    zoho_df['Currency Code'] = 'EUR'
    zoho_df['Account'] = 'Caterboss Sales'
    zoho_df['item Tax'] = 'VAT'
    zoho_df['IteM Tax %'] = 23
    zoho_df['Trade'] = 'No'
    zoho_df['Channel'] = 'Caterboss'
    zoho_df['Branch'] = 'Head Office'
    zoho_df['Shipping Tax Name'] = 'VAT'
    zoho_df['Shipping Tax percentage'] = 23
    zoho_df['LCS'] = 'false'
    zoho_df['Sales Person'] = 'Musgraves Tonka'
    zoho_df['Terms'] = '60'
    zoho_df['Sales Order Number'] = 'MUSGRAVE'
    if 'Order number' in zoho_df.columns:
        zoho_df['Invoice Number'] = zoho_df['Order number']
        zoho_df['Subject'] = zoho_df['Order number']
    zoho_df['Payment Terms'] = 'Musgrave'
    for col in zoho_col_order:
        if col not in zoho_df.columns:
            zoho_df[col] = ""
    zoho_df = zoho_df[zoho_col_order]
    buffer = BytesIO()
    zoho_df.to_excel(buffer, index=False)
    buffer.seek(0)
    latest_zoho_xlsx = buffer.getvalue()
    zoho_download_link = "<a href='/download_zoho_xlsx' download='zoho_orders.xlsx'><button class='copy-btn' style='background:#0f9d58;right:auto;top:auto;position:relative;margin-bottom:1em;margin-left:1em;'>Download Zoho XLSX</button></a>"

    # --- 7. Stock file updates ---
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

    # --- 8. DPD CSV Generation ---
    try:
        dpd_template_df, dpd_col_headers, dpd_delim = get_dpd_template_columns(DPD_TEMPLATE_PATH)
        dpd_col_count = len(dpd_col_headers)
    except Exception as e:
        dpd_col_headers, latest_dpd_csv = [], None
        dpd_error_report_html = f"<b>Failed to load DPD template: {e}</b>"
        dpd_download_link = "<span style='color:#e87272;'>DPD template not loaded.</span>"
        html = f"""<div style='color:red;padding:1em'>{dpd_error_report_html}</div>"""
        # --- UNMATCHED REPORT ADDED HERE ---
        if unmatched_report:
            report_html = """
            <div class="out-card" style="background:#fff4e5;border:1px solid #ffc107;">
              <h3>⚠️ Unmatched SKUs in Supplier.csv</h3>
              <table style="width:100%;border-collapse:collapse;">
                <tr><th>Order Number</th><th>Offer SKU</th><th>Quantity</th></tr>"""
            for item in unmatched_report:
                report_html += f"""
                <tr>
                  <td>{item['order_no']}</td>
                  <td>{item['sku']}</td>
                  <td>{item['qty']}</td>
                </tr>"""
            report_html += """
              </table>
            </div>
            """
            html += report_html
        return HTMLResponse(html)

    sku_limits = load_sku_limits()
    exclude_orders = set(['X001111531-A', 'X001111392-A', 'X001111558-A', 'X001111425-A'])
    exclude_orders.update([x.replace('-A', '-B') for x in exclude_orders])
    orders_df = df.copy()
    orders_df = orders_df[orders_df['Order number'].astype(str).str.endswith(('-A', '-B'))]
    orders_df = orders_df[~orders_df['Order number'].isin(exclude_orders)].copy()
    orders_df['base_order'] = orders_df['Order number'].str.replace(r'(-A|-B)$', '', regex=True)
    orders_df['order_suffix'] = orders_df['Order number'].str.extract(r'-(A|B)$')
    final_order_rows = []
    used_orders = set()
    grouped = orders_df.groupby('base_order')
    for base, group in grouped:
        has_A = (group['order_suffix'] == 'A').any()
        has_B = (group['order_suffix'] == 'B').any()
        row_A = group[group['order_suffix'] == 'A'].iloc[0] if has_A else None
        row_B = group[group['order_suffix'] == 'B'].iloc[0] if has_B else None
        if has_A and has_B:
            row = row_A.copy()
            sku = row.get('Offer SKU', '').strip().upper()
            qty = int(row.get('Quantity', 1))
            max_per = sku_limits.get(sku)
            if max_per:
                row['dpd_parcel_count'] = (qty + int(max_per) - 1) // int(max_per)
            else:
                row['dpd_parcel_count'] = 2
            if row['Order number'] not in used_orders:
                final_order_rows.append(row)
                used_orders.add(row['Order number'])
        elif has_A:
            row = row_A.copy()
            sku = row.get('Offer SKU', '').strip().upper()
            qty = int(row.get('Quantity', 1))
            max_per = sku_limits.get(sku)
            if max_per:
                row['dpd_parcel_count'] = (qty + int(max_per) - 1) // int(max_per)
            else:
                row['dpd_parcel_count'] = 1
            if row['Order number'] not in used_orders:
                final_order_rows.append(row)
                used_orders.add(row['Order number'])
        elif has_B:
            row = row_B.copy()
            sku = row.get('Offer SKU', '').strip().upper()
            qty = int(row.get('Quantity', 1))
            max_per = sku_limits.get(sku)
            if max_per:
                row['dpd_parcel_count'] = (qty + int(max_per) - 1) // int(max_per)
            else:
                row['dpd_parcel_count'] = 1
            if row['Order number'] not in used_orders:
                final_order_rows.append(row)
                used_orders.add(row['Order number'])

    dpd_final_df = pd.DataFrame(final_order_rows).drop_duplicates('Order number')
    dpd_field_map = {
        0:  lambda row: row.get('Order number', ''),
        1:  lambda row: row.get('Shipping address company', ''),
        2:  lambda row: row.get('Shipping address company', ''),
        3:  lambda row: row.get('Shipping address street 1', ''),
        4:  lambda row: row.get('Shipping address street 2', ''),
        5:  lambda row: row.get('Shipping address city', ''),
        6:  lambda row: row.get('Shipping address state', ''),
        7:  lambda row: row.get('Shipping address zip', ''),
        8:  lambda row: '372',
        9:  lambda row: str(row.get('dpd_parcel_count', 1)),
        10: lambda row: '10',
        11: lambda row: 'N',
        12: lambda row: 'O',
        23: lambda row: row.get('Shipping address first name', ''),
        24: lambda row: row.get('Shipping address phone', ''),
        28: lambda row: '8130L3',
        30: lambda row: 'N',
        31: lambda row: 'N',
    }
    required_fields = [
        (0, 'Order number'),
        (1, 'Shipping address company'),
        (3, 'Shipping address street 1'),
        (5, 'Shipping address city'),
        (7, 'Shipping address zip'),
        (23, 'Shipping address first name'),
        (24, 'Shipping address phone'),
    ]
    export_rows = []
    errors = []
    for _, row in dpd_final_df.iterrows():
        row_data = [''] * dpd_col_count
        missing = []
        for idx, fname in required_fields:
            value = dpd_field_map[idx](row)
            if not value or pd.isnull(value) or str(value).strip() == '':
                missing.append(fname)
        if missing:
            errors.append({'Order number': row.get('Order number', ''), 'Missing': ', '.join(missing)})
            continue
        for i in range(dpd_col_count):
            if i in dpd_field_map:
                row_data[i] = dpd_field_map[i](row)
        export_rows.append(row_data)
    if errors:
        dpd_error_report_html = "<div class='out-card' style='background:#ffefef;border:1px solid #e87272;'><h3>DPD Label Export: Excluded Orders</h3><table style='width:100%;border-collapse:collapse;'><tr><th>Order Number</th><th>Missing Field(s)</th></tr>"
        for e in errors:
            dpd_error_report_html += f"<tr><td>{e['Order number']}</td><td>{e['Missing']}</td></tr>"
        dpd_error_report_html += "</table></div>"
    else:
        dpd_error_report_html = ""
    if export_rows:
        dpd_buffer = StringIO()
        pd.DataFrame(export_rows).to_csv(dpd_buffer, header=False, index=False, sep=dpd_delim)
        latest_dpd_csv = dpd_buffer.getvalue().encode('utf-8')
        dpd_download_link = "<a href='/download_dpd_csv' download='DPD_Export.csv'><button class='copy-btn' style='background:#ff9900;right:auto;top:auto;position:relative;margin-bottom:1em;margin-left:1em;'>Download DPD CSV</button></a>"
    else:
        latest_dpd_csv = None
        dpd_download_link = "<span style='color:#e87272;'>No valid DPD export labels generated for this file.</span>"

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
    
{nisbets_out}

    <div class="out-card">
      <h3>Ship from Stock</h3>
      <button class="copy-btn" onclick="navigator.clipboard.writeText(document.getElementById('stockout').innerText)">Copy</button>
      <pre id="stockout">{stock_out}</pre>
    </div>
    <div style='margin-top:2em;text-align:center;'>
        {zoho_download_link}<br>{dpd_download_link}
    </div>
    {dpd_error_report_html}
    """

    # --- 9. Unmatched SKU report at the end ---
    if unmatched_report:
        report_html = """
        <div class="out-card" style="background:#fff4e5;border:1px solid #ffc107;">
          <h3>⚠️ Unmatched SKUs in Supplier.csv</h3>
          <table style="width:100%;border-collapse:collapse;">
            <tr><th>Order Number</th><th>Offer SKU</th><th>Quantity</th></tr>"""
        for item in unmatched_report:
            report_html += f"""
            <tr>
              <td>{item['order_no']}</td>
              <td>{item['sku']}</td>
              <td>{item['qty']}</td>
            </tr>"""
        report_html += """
          </table>
        </div>
        """
        html += report_html

    return HTMLResponse(html)


# ===============================
#  Nisbets CSV Batch Download
# ===============================
from fastapi.responses import StreamingResponse

latest_nisbets_csv_batches = {}

@app.get("/download_nisbets_csv/{po_number}")
async def download_nisbets_csv(po_number: str):
    csv_bytes = latest_nisbets_csv_batches.get(po_number)
    if not csv_bytes:
        return HTMLResponse(f"<b>No Nisbets CSV batch {po_number} generated in this session yet.</b>", status_code=404)
    return StreamingResponse(
        BytesIO(csv_bytes),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=Nisbets_{po_number}.csv"}
    )

# ===============================
#  Zoho and DPD Downloads
# ===============================
@app.get("/download_zoho_xlsx")
async def download_zoho_xlsx():
    if not latest_zoho_xlsx:
        return HTMLResponse("<b>No Zoho XLSX generated in this session yet.</b>", status_code=404)
    return StreamingResponse(
        BytesIO(latest_zoho_xlsx),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=zoho_orders.xlsx"}
    )

@app.get("/download_dpd_csv")
async def download_dpd_csv():
    if not latest_dpd_csv:
        return HTMLResponse("<b>No DPD CSV generated in this session yet.</b>", status_code=404)
    return StreamingResponse(
        BytesIO(latest_dpd_csv),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=DPD_Export.csv"}
    )

# ===============================
#  Admin routes
# ===============================
@app.post("/admin/set-max-sku")
async def set_max_sku(request: Request, sku: str = Form(...), max_per_parcel: int = Form(...)):
    if not request.session.get("admin_logged_in"):
        return RedirectResponse("/admin-login", status_code=303)
    sku_limits = load_sku_limits()
    sku = sku.strip().upper()
    sku_limits[sku] = int(max_per_parcel)
    save_sku_limits(sku_limits)
    return RedirectResponse("/admin", status_code=303)
    
@app.post("/admin/delete-max-sku")
async def delete_max_sku(request: Request, sku: str = Form(...)):
    if not request.session.get("admin_logged_in"):
        return RedirectResponse("/admin-login", status_code=303)
    sku_limits = load_sku_limits()
    sku = sku.strip().upper()
    if sku in sku_limits:
        del sku_limits[sku]
        save_sku_limits(sku_limits)
    return RedirectResponse("/admin", status_code=303)

@app.post("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/", status_code=303)

@app.post("/admin/undo-stock-update")
async def undo_stock_update(request: Request):
    if not request.session.get("admin_logged_in"):
        return RedirectResponse("/admin-login", status_code=303)
    results = []
    for name, file_id in [("Nisbets", NISBETS_STOCK_FILE_ID), ("Nortons", NORTONS_STOCK_FILE_ID)]:
        prev_version_id = get_previous_version_id(file_id)
        if not prev_version_id:
            results.append(f"<li>{name}: <span style='color:red'>No previous version available.</span></li>")
            continue
        try:
            restore_file_version(file_id, prev_version_id)
            results.append(f"<li>{name}: <span style='color:green'>Stock file restored to previous version.</span></li>")
        except Exception as e:
            results.append(f"<li>{name}: <span style='color:red'>Restore failed: {e}</span></li>")
    html = f"""
    <h3>Undo Stock Update Result</h3>
    <ul>{''.join(results)}</ul>
    <a href="/admin"><button>Back to Admin Dashboard</button></a>
    """
    return HTMLResponse(html)

# ===============================
#  Musgraves DPD Upload Tool
# ===============================
@app.get("/admin/musgraves-dpd-upload", response_class=HTMLResponse)
async def musgraves_dpd_form(request: Request):
    if not request.session.get("admin_logged_in"):
        return RedirectResponse("/admin-login", status_code=303)
    return """
    <style>
      body { font-family: 'Segoe UI',Arial,sans-serif; background: #f3f6f9;}
      .container { max-width: 480px; margin: 5em auto; background: #fff; border-radius: 14px; box-shadow: 0 2px 16px #0001; padding: 2.5em;}
      input,button {font-size:1.1em;padding:0.5em;margin:0.3em 0;width:100%;}
      button { background: #3b82f6; color: #fff; border: none; border-radius: 6px;}
    </style>
    <div class="container">
      <h2>Upload DPD Consignment File</h2>
      <form action="/admin/musgraves-dpd-upload" method="post" enctype="multipart/form-data">
        <input type="file" name="file" accept=".csv" required>
        <button type="submit">Generate Musgraves Upload File</button>
      </form>
      <div style="margin-top:1em;">
        <a href="/admin">← Back to Admin Dashboard</a>
      </div>
    </div>
    """

@app.post("/admin/musgraves-dpd-upload")
async def musgraves_dpd_upload(request: Request, file: UploadFile = File(...)):
    if not request.session.get("admin_logged_in"):
        return RedirectResponse("/admin-login", status_code=303)
    try:
        df = pd.read_csv(file.file)
        required_cols = ['DPD Customers First Ref', 'DPD Consignment number', 'cURL']
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            return HTMLResponse(f"<b>Missing required columns: {', '.join(missing)}</b>", status_code=400)
        export = pd.DataFrame({
            'order-id': df['DPD Customers First Ref'],
            'carrier-code': 'DPD',
            'carrier-standard-code': '',
            'carrier-name': 'DPD',
            'carrier-url': df['cURL'],
            'tracking-number': df['DPD Consignment number'],
        })
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode='w', newline='', encoding='utf-8') as tmpf:
            export.to_csv(tmpf, index=False)
            tmpf_path = tmpf.name
        return FileResponse(
            tmpf_path,
            filename="Mapped_DPD_Data_File.csv",
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=Mapped_DPD_Data_File.csv"}
        )
    except Exception as e:
        return HTMLResponse(f"<b>Failed: {e}</b>", status_code=500)
# --- Nisbets PO/SKU → Order Lookup Tool ---

from fastapi.responses import HTMLResponse
from fastapi import Request, Form

@app.post("/admin/lookup", response_class=HTMLResponse)
async def po_lookup_post(request: Request, po_number: str = Form(...), sku: str = Form(...)):
    if not request.session.get("admin_logged_in"):
        return RedirectResponse("/admin-login", status_code=303)
    sku = sku.strip().upper()
    po_number = po_number.strip()
    try:
        po_map = download_po_map()
    except Exception:
        return HTMLResponse("<b>No PO map log found or failed to load from OneDrive.</b>")
    batch = po_map.get(po_number)
    if not batch:
        return HTMLResponse(f"<b>No batch found for PO number: {po_number}</b>")
    # Look for matching SKU(s)
    orders = [row["Order Number"] for row in batch if row["Offer SKU"].strip().upper() == sku]
    if not orders:
        result = f"No order found for SKU <b>{sku}</b> in PO <b>{po_number}</b>."
    else:
        result = f"<b>Order(s) for SKU <b>{sku}</b> in PO <b>{po_number}</b>:</b><br>" + "<br>".join(orders)
    return f"""
    <style>.lookup-container {{ max-width:400px;margin:4em auto;background:#fff;border-radius:12px;box-shadow:0 2px 12px #0002;padding:2em;font-family:'Segoe UI',Arial,sans-serif;}}</style>
    <div class="lookup-container">
        <h2>Find Order Number by PO and SKU</h2>
        <form method="post" action="/admin/lookup">
            <label>PO Number:<br><input name="po_number" value="{po_number}" style="width:100%;"></label>
            <label>SKU:<br><input name="sku" value="{sku}" style="width:100%;"></label>
            <button type="submit" style="margin-top:1.2em;">Lookup</button>
        </form>
        <div style="margin:1.5em 0 0.5em 0; color:#193; font-weight:bold;">{result}</div>
        <div style="margin-top:1.5em;"><a href="/admin">← Back to Admin Dashboard</a></div>
    </div>
    """


@app.post("/admin/lookup", response_class=HTMLResponse)
async def po_lookup_post(request: Request, po_number: str = Form(...), sku: str = Form(...)):
    if not request.session.get("admin_logged_in"):
        return RedirectResponse("/admin-login", status_code=303)
    sku = sku.strip().upper()
    po_number = po_number.strip()
    # Load the PO mapping file
def upload_po_map(po_map):
    token = get_graph_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    data = json.dumps(po_map).encode("utf-8")
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{PO_MAP_FILE_ID}/content"
    r = requests.put(url, headers=headers, data=data)
    r.raise_for_status()
def download_po_map():
    token = get_graph_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{PO_MAP_FILE_ID}/content"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return json.loads(r.content.decode())
    batch = po_map.get(po_number)
    if not batch:
        return HTMLResponse(f"<b>No batch found for PO number: {po_number}</b>")
    # Look for matching SKU(s)
    orders = [row["Order Number"] for row in batch if row["Offer SKU"].strip().upper() == sku]
    if not orders:
        result = f"No order found for SKU <b>{sku}</b> in PO <b>{po_number}</b>."
    else:
        result = f"<b>Order(s) for SKU <b>{sku}</b> in PO <b>{po_number}</b>:</b><br>" + "<br>".join(orders)
    return f"""
    <style>.lookup-container {{ max-width:400px;margin:4em auto;background:#fff;border-radius:12px;box-shadow:0 2px 12px #0002;padding:2em;font-family:'Segoe UI',Arial,sans-serif;}}</style>
    <div class="lookup-container">
        <h2>Find Order Number by PO and SKU</h2>
        <form method="post" action="/admin/lookup">
            <label>PO Number:<br><input name="po_number" value="{po_number}" style="width:100%;"></label>
            <label>SKU:<br><input name="sku" value="{sku}" style="width:100%;"></label>
            <button type="submit" style="margin-top:1.2em;">Lookup</button>
        </form>
        <div style="margin:1.5em 0 0.5em 0; color:#193; font-weight:bold;">{result}</div>
        <div style="margin-top:1.5em;"><a href="/admin">← Back to Admin Dashboard</a></div>
    </div>
    """


import os
import requests
import pandas as pd
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import HTMLResponse, StreamingResponse
from io import BytesIO, StringIO

TENANT_ID = os.getenv("TENANT_ID", "ce280aae-ee92-41fe-ab60-66b37ebc97dd")
CLIENT_ID = os.getenv("CLIENT_ID", "83acd574-ab02-4cfe-b28c-e38c733d9a52")
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "FYX8Q~bZVXuKEenMTryxYw-ZuQOq2OBTNIu8Qa~i")
DRIVE_ID = os.getenv("DRIVE_ID", "b!udRZ7OsrmU61CSAYEn--q1fPtuPR3TZAsv2B9cCW-gzWb8B-lsUaQLURaNYNJxjP")
SUPPLIER_FILE_ID = os.getenv("SUPPLIER_FILE_ID", "01YTGSV5DGZEMEISWEYVDJRULO4ADDVCVQ")
NISBETS_STOCK_FILE_ID = os.getenv("NISBETS_STOCK_FILE_ID", "01YTGSV5HJCNBDXINJP5FJE2TICQ6Q3NEX")
NORTONS_STOCK_FILE_ID = os.getenv("NORTONS_STOCK_FILE_ID", "01YTGSV5FBVS7JYODGLREKL273FSJ3XRLP")

ZOHO_TEMPLATE_PATH = "column format.xlsx"
DPD_TEMPLATE_PATH = "DPD.Import(1).csv"   # Use the correct file!

app = FastAPI()
from starlette.middleware.sessions import SessionMiddleware
app.add_middleware(SessionMiddleware, secret_key="!supersecret!")  # Put any secret key here

latest_nisbets_csv = None
latest_zoho_xlsx = None
latest_dpd_csv = None
dpd_error_report_html = ""

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
    # Check delimiter in file
    with open(template_path, "r", encoding="utf-8") as f:
        sample = f.read(2048)
    delimiter = "," if sample.count(",") > sample.count(";") else ";"
    print(f"DEBUG: DPD template detected delimiter: '{delimiter}'")
    df = pd.read_csv(template_path, header=None, delimiter=delimiter)
    headers = list(df.iloc[1])
    return df, headers, delimiter

@app.get("/", response_class=HTMLResponse)
async def main_upload_form(request: Request):
    if request.session.get("admin_logged_in"):
        # --- Your existing upload form HTML goes here! ---
        return """
        <style>
        body { font-family: 'Segoe UI',Arial,sans-serif; background: #f3f6f9; margin: 0; padding: 0;}
        .container { max-width: 720px; margin: 3em auto; background: #fff; border-radius: 14px; box-shadow: 0 2px 16px #0001; padding: 2.5em;}
        h2 { margin-bottom: 0.5em; }
        .upload-form { display: flex; flex-direction: column; gap: 1em;}
        button { background: #3b82f6; color: #fff; border: none; border-radius: 6px; font-size: 1.1em; padding: 0.7em 2em; cursor: pointer;}
        button:hover { background: #2563eb; }
        .footer { margin-top: 2em; text-align: center; color: #888;}
        </style>
        <div class="container">
          <form action="/logout" method="post" style="text-align:right;">
            <button type="submit" style="background:#e53e3e;">Logout</button>
          </form>
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
    else:
        # Show login form
        return """
        <style>
        body { font-family: 'Segoe UI',Arial,sans-serif; background: #f3f6f9; }
        .container { max-width: 420px; margin: 5em auto; background: #fff; border-radius: 14px; box-shadow: 0 2px 16px #0001; padding: 2.5em;}
        input,button {font-size:1.1em;padding:0.5em;margin:0.3em 0;width:100%;}
        button { background: #3b82f6; color: #fff; border: none; border-radius: 6px;}
        .footer { margin-top: 2em; text-align: center; color: #888;}
        </style>
        <div class="container">
          <h2>Admin Login</h2>
          <form action="/login" method="post">
            <input type="password" name="password" placeholder="Password" required>
            <button type="submit">Login</button>
          </form>
        </div>
        <div class="footer">Caterboss Orders &copy; 2025</div>
        """
@app.post("/login")
async def login(request: Request, password: str = Form(...)):
    if password == "Admin123":
        request.session["admin_logged_in"] = True
        return RedirectResponse("/", status_code=303)
    else:
        return HTMLResponse(
            "<h3>Invalid password. <a href='/'>Try again</a>.</h3>",
            status_code=401,
        )
@app.post("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/", status_code=303)


@app.post("/upload_orders/display")
async def upload_orders_display(file: UploadFile = File(...)):
    global latest_nisbets_csv, latest_zoho_xlsx, latest_dpd_csv, dpd_error_report_html
    try:
        df = pd.read_excel(file.file)
        print(f"Total orders in upload: {len(df)}")
        print("Order file columns:", list(df.columns))
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
        latest_nisbets_csv = csv_buffer.getvalue().encode('utf-8')
        download_link = "<a href='/download_nisbets_csv' download='Nisbets.csv'><button class='copy-btn' style='right:auto;top:auto;position:relative;margin-bottom:1em;'>Download Nisbets CSV</button></a>"
    else:
        latest_nisbets_csv = None
        download_link = ""

    # --- Build Zoho XLSX (template column order)
    try:
        template_df = pd.read_excel(ZOHO_TEMPLATE_PATH)
        zoho_col_order = list(template_df.columns)
    except Exception as e:
        return HTMLResponse(f"<b>Failed to load Zoho template: {e}</b>", status_code=500)

    zoho_df = df.copy()
    # Add your fields and set default values
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

    # Ensure all template columns exist (fill missing with blank)
    for col in zoho_col_order:
        if col not in zoho_df.columns:
            zoho_df[col] = ""
    # Now set DataFrame to exact template column order
    zoho_df = zoho_df[zoho_col_order]
    buffer = BytesIO()
    zoho_df.to_excel(buffer, index=False)
    buffer.seek(0)
    latest_zoho_xlsx = buffer.getvalue()
    zoho_download_link = "<a href='/download_zoho_xlsx' download='zoho_orders.xlsx'><button class='copy-btn' style='background:#0f9d58;right:auto;top:auto;position:relative;margin-bottom:1em;margin-left:1em;'>Download Zoho XLSX</button></a>"

    # Update stock DataFrames
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

    # ----------------- DPD LABEL CSV GENERATION --------------------
    try:
        dpd_template_df, dpd_col_headers, dpd_delim = get_dpd_template_columns(DPD_TEMPLATE_PATH)
        dpd_mandatory_row = list(dpd_template_df.iloc[2])
    except Exception as e:
        dpd_col_headers = []
        dpd_mandatory_row = []
        latest_dpd_csv = None
        dpd_error_report_html = f"<b>Failed to load DPD template: {e}</b>"

    dpd_col_count = len(dpd_col_headers)
    print(f"DPD template columns: {dpd_col_headers}")
    print(f"DPD col count: {dpd_col_count}")

    exclude_orders = set(['X001111531-A', 'X001111392-A', 'X001111558-A', 'X001111425-A'])
    exclude_orders.update([x.replace('-A', '-B') for x in exclude_orders])

    orders_df = df.copy()
    print(f"Initial upload file rows: {len(orders_df)}")
    orders_df = orders_df[orders_df['Order number'].astype(str).str.endswith(('-A', '-B'))]
    print(f"After -A/-B filter: {len(orders_df)}")
    orders_df = orders_df[~orders_df['Order number'].isin(exclude_orders)].copy()
    print(f"After removing sample/fraud: {len(orders_df)}")

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
            row['dpd_parcel_count'] = 2
            if row['Order number'] not in used_orders:
                final_order_rows.append(row)
                used_orders.add(row['Order number'])
        elif has_A:
            row = row_A.copy()
            row['dpd_parcel_count'] = 1
            if row['Order number'] not in used_orders:
                final_order_rows.append(row)
                used_orders.add(row['Order number'])
        elif has_B:
            row = row_B.copy()
            row['dpd_parcel_count'] = 1
            if row['Order number'] not in used_orders:
                final_order_rows.append(row)
                used_orders.add(row['Order number'])

    dpd_final_df = pd.DataFrame(final_order_rows).drop_duplicates('Order number')
    print(f"Orders after dedupe: {len(dpd_final_df)}")
    print("First 5 orders after dedupe:", dpd_final_df.head().to_dict())

    dpd_field_map = {
        0:  lambda row: row.get('Order number', ''),
        1:  lambda row: row.get('Shipping address company', ''),
        2:  lambda row: row.get('Shipping address company', ''),
        3:  lambda row: row.get('Shipping address street 1', ''),
        4:  lambda row: row.get('Shipping address street 2', ''),
        5:  lambda row: row.get('Shipping address city', ''),
        6:  lambda row: row.get('Shipping address state', ''),
        7:  lambda row: row.get('Shipping address zip', ''),
        8:  lambda row: '372',       # Always
        9:  lambda row: str(row.get('dpd_parcel_count', 1)),
        10: lambda row: '10',        # Always
        11: lambda row: 'N',         # Always
        12: lambda row: 'O',         # Always
        23: lambda row: row.get('Shipping address first name', ''),
        24: lambda row: row.get('Shipping address phone', ''),
        28: lambda row: '8130L3',    # Always
        30: lambda row: 'N',         # Always
        31: lambda row: 'N',         # Always
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
    print(f"DPD orders to export: {len(export_rows)}")
    print(f"DPD errors (missing fields): {len(errors)}")

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
        {zoho_download_link}<br>{dpd_download_link}
    </div>
    {dpd_error_report_html}
    """
    return HTMLResponse(html)

@app.get("/download_nisbets_csv")
async def download_nisbets_csv():
    if not latest_nisbets_csv:
        return HTMLResponse("<b>No Nisbets CSV generated in this session yet.</b>", status_code=404)
    return StreamingResponse(
        BytesIO(latest_nisbets_csv),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=Nisbets.csv"}
    )

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
from fastapi.staticfiles import StaticFiles

app.mount("/static", StaticFiles(directory="static"), name="static")

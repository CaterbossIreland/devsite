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

app = FastAPI()
latest_nisbets_csv = None

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

@app.get("/", response_class=HTMLResponse)
async def main_upload_form():
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
      <h2>Upload Orders File</h2>
      <form class="upload-form" id="uploadForm" enctype="multipart/form-data">
        <input name="file" type="file" accept=".xlsx" required>
        <button type="submit">Upload & Show Output</button>
      </form>
      <div id="results"></div>
      <hr style='margin:2em 0;'>
      <h3>Convert Orders File for Zoho Books Import</h3>
      <form id="zohoForm" enctype="multipart/form-data">
        <input name="file" type="file" accept=".xlsx" required>
        <button type="submit" style="background:#0f9d58;margin-left:1em;">Convert & Download Zoho XLSX</button>
      </form>
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
    document.getElementById('zohoForm').onsubmit = async function(e){
      e.preventDefault();
      let formData = new FormData(this);
      let res = await fetch('/convert_for_zoho', { method: 'POST', body: formData });
      if(res.status == 200){
        let blob = await res.blob();
        let url = window.URL.createObjectURL(blob);
        let a = document.createElement('a');
        a.href = url;
        a.download = 'zoho_orders.xlsx';
        document.body.appendChild(a);
        a.click();
        a.remove();
      }else{
        alert('Zoho XLSX conversion failed');
      }
    }
    </script>
    """

@app.post("/upload_orders/display")
async def upload_orders_display(file: UploadFile = File(...)):
    global latest_nisbets_csv
    try:
        df = pd.read_excel(file.file)
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

    # --- NEW: Build Nisbets.csv
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

@app.post("/convert_for_zoho")
async def convert_for_zoho(file: UploadFile = File(...)):
    df = pd.read_excel(file.file)
    if 'Date created' in df.columns:
        df['Date created'] = df['Date created'].astype(str).str.split().str[0]
    df['Shipping total amount'] = 4.95
    df['Currency Code'] = 'EUR'
    df['Account'] = 'Caterboss Sales'
    df['item Tax'] = 'VAT'
    df['IteM Tax %'] = 23
    df['Trade'] = 'No'
    df['Channel'] = 'Caterboss'
    df['Branch'] = 'Head Office'
    df['Shipping Tax Name'] = 'VAT'
    df['Shipping Tax percentage'] = 23
    df['LCS'] = 'false'
    df['Sales Person'] = 'Musgraves Tonka'
    df['Terms'] = '60'
    df['Sales Order Number'] = 'MUSGRAVE'
    if 'Order number' in df.columns:
        df['Invoice Number'] = df['Order number']
        df['Subject'] = df['Order number']
    df['Payment Terms'] = 'Musgrave'
    col_order = [
        'Order number', 'Invoice Number', 'Subject', 'Date created', 'Shipping total amount',
        'Currency Code', 'Account', 'item Tax', 'IteM Tax %', 'Trade', 'Channel', 'Branch',
        'Shipping Tax Name', 'Shipping Tax percentage', 'LCS', 'Sales Person', 'Terms',
        'Sales Order Number', 'Payment Terms'
    ]
    df = df[[c for c in col_order if c in df.columns] + [c for c in df.columns if c not in col_order]]
    buffer = BytesIO()
    df.to_excel(buffer, index=False)
    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=zoho_orders.xlsx"}
    )

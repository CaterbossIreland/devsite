import os
import requests
import pandas as pd
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import HTMLResponse
from io import BytesIO

TENANT_ID = os.getenv("TENANT_ID", "ce280aae-ee92-41fe-ab60-66b37ebc97dd")
CLIENT_ID = os.getenv("CLIENT_ID", "83acd574-ab02-4cfe-b28c-e38c733d9a52")
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "FYX8Q~bZVXuKEenMTryxYw-ZuQOq2OBTNIu8Qa~i")
DRIVE_ID = os.getenv("DRIVE_ID", "b!udRZ7OsrmU61CSAYEn--q1fPtuPR3TZAsv2B9cCW-gzWb8B-lsUaQLURaNYNJxjP")
SUPPLIER_FILE_ID = os.getenv("SUPPLIER_FILE_ID", "01YTGSV5DGZEMEISWEYVDJRULO4ADDVCVQ")

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

def download_supplier_csv():
    token = get_graph_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{SUPPLIER_FILE_ID}/content"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return pd.read_csv(BytesIO(r.content))

app = FastAPI()

@app.get("/", response_class=HTMLResponse)
async def main_upload_form():
    return """
    <style>
    body { font-family: 'Segoe UI',Arial,sans-serif; background: #f3f6f9; margin: 0; padding: 0;}
    .container { max-width: 700px; margin: 3em auto; background: #fff; border-radius: 14px; box-shadow: 0 2px 16px #0001; padding: 2.5em;}
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

@app.post("/upload_orders/display")
async def upload_orders_display(file: UploadFile = File(...)):
    try:
        df = pd.read_excel(file.file)
    except Exception as e:
        return HTMLResponse(f"<b>Order file read failed:</b> {e}", status_code=500)
    try:
        supplier_df = download_supplier_csv()
    except Exception as e:
        return HTMLResponse(f"<b>Supplier fetch failed:</b> {e}", status_code=500)
    try:
        orders = df[['Order number', 'Offer SKU', 'Quantity']].dropna()
    except Exception as e:
        return HTMLResponse(f"<b>Missing columns in order file:</b> {e}", status_code=500)
    try:
        sku_to_supplier = dict(zip(supplier_df['Offer SKU'], supplier_df['Supplier Name']))
        orders['Supplier Name'] = orders['Offer SKU'].map(sku_to_supplier)
    except Exception as e:
        return HTMLResponse(f"<b>Failed to map SKUs to suppliers:</b> {e}", status_code=500)
    try:
        def make_nice_block(supplier):
            supplier_orders = orders[orders['Supplier Name'] == supplier]
            grouped = supplier_orders.groupby('Order number')
            out = []
            for order, group in grouped:
                out.append(f"Order Number: {order}\n")
                for _, row in group.iterrows():
                    out.append(f"·        {int(row['Quantity'])}x {row['Offer SKU']}\n")
                out.append("\n------------------------------\n\n")
            return "".join(out) if out else "No orders for this supplier."

        nout = make_nice_block('Nortons')
        niout = make_nice_block('Nisbets')
        html = f"""
        <style>
        .out-card {{ background:#f7fafc; border-radius:10px; margin:1.5em 0; padding:1.3em 1.5em; box-shadow:0 2px 8px #0001; position:relative;}}
        .copy-btn {{ position:absolute; right:24px; top:26px; background:#3b82f6; color:#fff; border:none; border-radius:4px; padding:5px 15px; cursor:pointer; font-size:1em;}}
        .copy-btn:hover {{ background:#2563eb; }}
        h3 {{ margin-top:0; }}
        pre {{ white-space: pre-wrap; font-family:inherit; font-size:1.09em; margin:0;}}
        </style>
        <div class="out-card">
          <h3>Nortons</h3>
          <button class="copy-btn" onclick="navigator.clipboard.writeText(document.getElementById('nortonsout').innerText)">Copy</button>
          <pre id="nortonsout">{nout}</pre>
        </div>
        <div class="out-card">
          <h3>Nisbets</h3>
          <button class="copy-btn" onclick="navigator.clipboard.writeText(document.getElementById('nisbetsout').innerText)">Copy</button>
          <pre id="nisbetsout">{niout}</pre>
        </div>
        """
        return HTMLResponse(html)
    except Exception as e:
        return HTMLResponse(f"<b>Failed during output formatting:</b> {e}", status_code=500)

from fastapi import UploadFile, File, HTTPException
from fastapi.responses import FileResponse
from io import BytesIO
from tempfile import TemporaryDirectory
import pandas as pd
import os
import zipfile
from main import download_excel_file, download_csv_file, STOCK_FILE_IDS, SUPPLIER_FILE_ID

# Aliases for SKU detection (uppercase keys)
COLUMN_ALIASES = {
    "ORDER NO": "ORDER",
    "ORDER NUMBER": "ORDER",
    "ORDER#": "ORDER",
    "PRODUCT CODE": "SKU",
    "ITEM CODE": "SKU",
    "OFFER SKU": "SKU"
}

QTY_ALIASES = ["QUANTITY", "QTY", "QTY.", "QTY ORDERED"]

@app.post("/generate-docs/")
async def generate_docs(file: UploadFile = File(...)):
    try:
        uploaded_orders = pd.read_excel(BytesIO(await file.read()))

        # Normalize columns to uppercase for detection
        normalized_columns = {col.strip().upper(): col for col in uploaded_orders.columns}

        # Detect SKU and QTY columns using aliases
        sku_col = next((normalized_columns.get(alias) for alias in COLUMN_ALIASES if alias in normalized_columns), None)
        qty_col = next((normalized_columns.get(alias) for alias in QTY_ALIASES if alias in normalized_columns), None)

        if not sku_col or not qty_col:
            raise HTTPException(status_code=400, detail="400: SKU or QTY column missing in uploaded file")

        # Rename to consistent column names for processing
        uploaded_orders.rename(columns={sku_col: "SKU", qty_col: "QTY"}, inplace=True)

        # Load supplier map from OneDrive
        supplier_df = download_csv_file(SUPPLIER_FILE_ID)
        supplier_map = supplier_df.set_index("SKU")["Supplier"].to_dict()

        # Filter out SKUs already in stock across all stock files
        needed_orders = uploaded_orders.copy()
        for file_id in STOCK_FILE_IDS:
            stock_df = download_excel_file(file_id)
            stock_skus = set(stock_df["SKU"].astype(str).str.strip().unique())
            needed_orders = needed_orders[~needed_orders["SKU"].astype(str).str.strip().isin(stock_skus)]

        # Group orders by supplier
        supplier_orders = {}
        for _, row in needed_orders.iterrows():
            sku = str(row["SKU"]).strip()
            supplier = supplier_map.get(sku, "Unknown")
            supplier_orders.setdefault(supplier, []).append(row)

        # Create a zip archive of supplier order Excel files
        with TemporaryDirectory() as tmpdir:
            zip_path = os.path.join(tmpdir, "supplier_orders.zip")
            with zipfile.ZipFile(zip_path, "w") as zipf:
                for supplier, rows in supplier_orders.items():
                    df = pd.DataFrame(rows)
                    filename = f"{supplier}_order_list.xlsx"
                    filepath = os.path.join(tmpdir, filename)
                    df.to_excel(filepath, index=False)
                    zipf.write(filepath, arcname=filename)

            return FileResponse(zip_path, filename="supplier_orders.zip", media_type="application/zip")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

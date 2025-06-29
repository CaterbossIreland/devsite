from fastapi.responses import FileResponse
from tempfile import TemporaryDirectory
import zipfile

@app.post("/generate-docs/")
async def generate_docs(file: UploadFile = File(...)):
    try:
        uploaded_orders = pd.read_excel(BytesIO(await file.read()))

        # Normalize column names
        COLUMN_ALIASES = {
            "ORDER NO": "ORDER",
            "ORDER NUMBER": "ORDER",
            "ORDER#": "ORDER",
            "PRODUCT CODE": "SKU",
            "ITEM CODE": "SKU",
            "OFFER SKU": "SKU",
            "QUANTITY": "QTY",
            "QTY.": "QTY",
            "QTY ORDERED": "QTY"
        }
        uploaded_orders.columns = [
            COLUMN_ALIASES.get(col.strip().upper(), col.strip().upper())
            for col in uploaded_orders.columns
        ]

        if "SKU" not in uploaded_orders.columns or "QTY" not in uploaded_orders.columns:
            raise HTTPException(status_code=400, detail="400: SKU or QTY column missing in uploaded file")

        # Download supplier map
        supplier_df = download_csv_file(SUPPLIER_FILE_ID)
        supplier_map = supplier_df.set_index("SKU")["Supplier"].to_dict()

        # Load stock files and remove already in stock
        needed_orders = uploaded_orders.copy()
        for file_id in STOCK_FILE_IDS:
            stock_df = download_excel_file(file_id)
            stock_skus = set(stock_df["SKU"].astype(str).str.strip().unique())
            needed_orders = needed_orders[~needed_orders["SKU"].astype(str).str.strip().isin(stock_skus)]

        # Group by supplier
        supplier_orders = {}
        for _, row in needed_orders.iterrows():
            sku = str(row["SKU"]).strip()
            supplier = supplier_map.get(sku, "Unknown")
            supplier_orders.setdefault(supplier, []).append(row)

        # Create zip of generated Excel files
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

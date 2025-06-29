from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import pandas as pd
from io import BytesIO
import os
import uuid
import csv
from docx import Document

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# === Configuration ===
OUTPUT_DIR = "downloads"
os.makedirs(OUTPUT_DIR, exist_ok=True)
SUPPLIER_MAP_PATH = "supplier.csv.csv"

# === Helper to standardize columns ===
def normalize_columns(df):
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
    df.columns = [COLUMN_ALIASES.get(col.strip().upper(), col.strip().upper()) for col in df.columns]
    return df

# === Helper to create .docx from grouped SKUs ===
def create_doc(supplier_name, skus):
    doc = Document()
    doc.add_heading(f"Order for {supplier_name}", 0)
    table = doc.add_table(rows=1, cols=2)
    hdr_cells = table.rows[0].cells
    hdr_cells[0].text = 'SKU'
    hdr_cells[1].text = 'QTY'
    for item in skus:
        row = table.add_row().cells
        row[0].text = item['SKU']
        row[1].text = str(item['QTY'])
    filename = f"{supplier_name.replace(' ', '_')}_Orders.docx"
    path = os.path.join(OUTPUT_DIR, filename)
    doc.save(path)
    return filename, path

# === Helper to create CSV checklist for Nisbets ===
def create_checklist_csv(skus):
    filename = "Nisbets_Checklist.csv"
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["SKU", "QTY"])
        for item in skus:
            writer.writerow([item['SKU'], item['QTY']])
    return filename, path

@app.post("/generate_supplier_docs")
async def generate_supplier_docs(file: UploadFile = File(...)):
    try:
        order_data = await file.read()
        df = pd.read_excel(BytesIO(order_data))
        df = normalize_columns(df)

        if not {"SKU", "QTY"}.issubset(df.columns):
            raise HTTPException(status_code=400, detail="Missing required columns")

        supplier_df = pd.read_csv(SUPPLIER_MAP_PATH)
        supplier_df.columns = [col.strip().upper() for col in supplier_df.columns]

        if not {"SKU", "SUPPLIER"}.issubset(supplier_df.columns):
            raise HTTPException(status_code=400, detail="Supplier map must have SKU and SUPPLIER columns")

        merged = df.merge(supplier_df, how="left", on="SKU")

        unmatched = merged[merged["SUPPLIER"].isna()][["SKU", "QTY"]].to_dict(orient="records")

        matched = merged.dropna(subset=["SUPPLIER"])
        grouped = matched.groupby("SUPPLIER")

        files = []
        response_preview = {}

        for supplier, group in grouped:
            skus = group.groupby("SKU")["QTY"].sum().reset_index().to_dict(orient="records")
            doc_filename, doc_path = create_doc(supplier, skus)
            files.append({"name": doc_filename, "url": f"/downloads/{doc_filename}"})

            if supplier.lower() == "nisbets":
                csv_filename, csv_path = create_checklist_csv(skus)
                files.append({"name": csv_filename, "url": f"/downloads/{csv_filename}"})

            # Include doc preview
            preview_lines = [f"SKU: {item['SKU']} - QTY: {item['QTY']}" for item in skus]
            response_preview[supplier] = preview_lines

        return JSONResponse(content={
            "status": "success",
            "files": files,
            "preview": response_preview,
            "unmatched_skus": unmatched
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/downloads/{filename}")
def download_file(filename: str):
    file_path = os.path.join(OUTPUT_DIR, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(file_path)

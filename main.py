from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from io import BytesIO
import openpyxl

app = FastAPI()

# CORS setup (allow all origins for now)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Define acceptable aliases for the SKU column
COLUMN_ALIASES = {
    "PRODUCT CODE": "SKU",
    "ITEM CODE": "SKU",
    "OFFER SKU": "SKU",
    "SKU": "SKU"
}

# Endpoint
@app.post("/generate-docs/")
async def generate_docs(file: UploadFile = File(...)):
    try:
        # Read the Excel file into pandas
        contents = await file.read()
        df = pd.read_excel(BytesIO(contents), engine="openpyxl")

        # Normalize and map column names
        normalized_columns = {col.strip().upper(): col for col in df.columns}
        sku_col = next((normalized_columns.get(alias.upper()) for alias in COLUMN_ALIASES if alias.upper() in normalized_columns), None)

        if not sku_col:
            raise HTTPException(status_code=400, detail="None of ['SKU'] are in the columns")

        # Group and summarize
        summary = df.groupby(sku_col).size().reset_index(name="Quantity")
        summary = summary.rename(columns={sku_col: "SKU"})

        # Save result to in-memory Excel
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            summary.to_excel(writer, index=False, sheet_name="Supplier Order")
        output.seek(0)

        # Return downloadable file
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=supplier-order.xlsx"},
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

import pandas as pd
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import PlainTextResponse

app = FastAPI()

@app.post("/upload_orders/", response_class=PlainTextResponse)
async def upload_orders(file: UploadFile = File(...)):
    df = pd.read_excel(file.file)
    # Select only the needed columns, drop blanks
    out = df[['Order number', 'Offer SKU', 'Quantity']].dropna()
    lines = [
        f"{row['Order number']}, {row['Offer SKU']}, {row['Quantity']}"
        for _, row in out.iterrows()
    ]
    # Optional: add a header row if you want
    # lines.insert(0, "Order number, Offer SKU, Quantity")
    return "\n".join(lines)

import pandas as pd
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import PlainTextResponse

app = FastAPI()

@app.post("/upload_orders/", response_class=PlainTextResponse)
async def upload_orders(file: UploadFile = File(...)):
    df = pd.read_excel(file.file)
    df = df[['Order number', 'Offer SKU', 'Quantity']].dropna()
    grouped = df.groupby('Order number')
    result = []
    for order, group in grouped:
        result.append(f"Order Number: {order}\n")
        for _, row in group.iterrows():
            result.append(f"·        {int(row['Quantity'])}x {row['Offer SKU']}\n")
        result.append("\n------------------------------\n")
    return "".join(result)

from fastapi import FastAPI, UploadFile, File
import uvicorn

app = FastAPI()

@app.post("/check_stock")
async def check_stock(order_csv: UploadFile = File(...)):
    contents = await order_csv.read()
    return {"message": "Order file received", "filename": order_csv.filename}

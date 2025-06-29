import pandas as pd
from io import BytesIO
from graph_api_auth import get_graph_client

# Define the function to update stock

def upload_stock_update(stock_df: pd.DataFrame, items: dict) -> pd.DataFrame:
    updated_rows = 0
    for sku, quantity in items.items():
        match = stock_df[stock_df["SKU"].astype(str).str.strip() == str(sku).strip()]
        if not match.empty:
            stock_df.loc[match.index, "QTY"] = quantity
            updated_rows += 1
        else:
            new_row = pd.DataFrame({"SKU": [sku], "QTY": [quantity]})
            stock_df = pd.concat([stock_df, new_row], ignore_index=True)
            updated_rows += 1
    return stock_df


def download_excel_file(drive_id: str, file_id: str) -> pd.DataFrame:
    client = get_graph_client()
    response = client.get(f"/drives/{drive_id}/items/{file_id}/content")
    if response.status_code != 200:
        raise Exception("Failed to download Excel file from OneDrive")
    return pd.read_excel(BytesIO(response.content), engine="openpyxl")


def update_excel_file(drive_id: str, file_id: str, df: pd.DataFrame):
    client = get_graph_client()
    buffer = BytesIO()
    df.to_excel(buffer, index=False, engine="openpyxl")
    buffer.seek(0)
    upload_url = f"/drives/{drive_id}/items/{file_id}/content"
    response = client.put(upload_url, data=buffer.read())
    if response.status_code not in (200, 201):
        raise Exception("Failed to upload updated Excel file to OneDrive")

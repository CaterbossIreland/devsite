import httpx
import pandas as pd
from io import BytesIO

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# Util to download an Excel file and read its contents
async def download_excel_file(token: str, site_id: str, drive_id: str, item_id: str) -> pd.DataFrame:
    headers = {"Authorization": f"Bearer {token}"}

    # Download the Excel file binary
    url = f"{GRAPH_BASE}/sites/{site_id}/drives/{drive_id}/items/{item_id}/content"
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=headers)
        resp.raise_for_status()
        file_bytes = BytesIO(resp.content)

    # Load as Excel
    df = pd.read_excel(file_bytes)
    if 'SKU' not in df.columns or 'Quantity' not in df.columns:
        raise ValueError(f"Missing SKU or Quantity column in file {item_id}")
    return df[['SKU', 'Quantity']]

# Async version of load_stock_data
async def load_stock_data(site_id: str, drive_id: str, file_ids: list[str], token: str) -> pd.DataFrame:
    all_data = []
    for fid in file_ids:
        df = await download_excel_file(token, site_id, drive_id, fid)
        all_data.append(df)
    combined = pd.concat(all_data)
    return combined.groupby('SKU', as_index=False).sum()  # Summed quantities for same SKU
def check_stock_availability(orders_df: pd.DataFrame, stock_df: pd.DataFrame) -> pd.DataFrame:
    # Ensure columns are standardized
    orders = orders_df.rename(columns=lambda x: x.strip())
    stock = stock_df.rename(columns=lambda x: x.strip())

    # Aggregate duplicate SKUs in orders
    orders_grouped = orders.groupby("SKU", as_index=False).agg({"Quantity": "sum"}).rename(columns={"Quantity": "ordered_qty"})
    stock = stock.rename(columns={"Quantity": "stock_qty"})

    # Merge both sets
    merged = pd.merge(orders_grouped, stock, on="SKU", how="left").fillna(0)

    # Calculate what we can ship and what we need to order
    merged["from_stock"] = merged.apply(lambda row: min(row["ordered_qty"], row["stock_qty"]), axis=1)
    merged["to_order"] = merged["ordered_qty"] - merged["from_stock"]

    return merged

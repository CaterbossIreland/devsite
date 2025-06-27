from fastapi import FastAPI
from graph_files import list_onedrive_root_files

app = FastAPI()

@app.get("/files")
def get_files():
    try:
        data = list_onedrive_root_files()
        return data
    except Exception as e:
        return {"error": str(e)}

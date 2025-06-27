from fastapi import FastAPI
from graph_files import list_onedrive_root_files

app = FastAPI()

@app.get("/")
def list_files():
    return list_onedrive_root_files()

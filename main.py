from fastapi import FastAPI
from graph_files import list_onedrive_root_files

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Server is up and running."}

@app.get("/files")
def get_files():
    return list_onedrive_root_files()

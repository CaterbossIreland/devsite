from fastapi import FastAPI
from graph_auth import get_access_token

app = FastAPI()

@app.get("/")
def root():
    try:
        token = get_access_token()
        return {"access_token": token[:20] + "..."}
    except Exception as e:
        return {"error": str(e)}

import requests

def get_access_token():
    tenant_id = "ce280aae-ee92-41fe-ab60-66b37ebc97dd"
    client_id = "83acd574-ab02-4cfe-b28c-e38c733d9a52"
    client_secret = "FYX8Q~bZVXuKEenMTnyxYw-ZuQqo2DBTlNU8Qav1"

    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

    data = {
        "client_id": client_id,
        "scope": "https://graph.microsoft.com/.default",
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    }

    response = requests.post(url, data=data)
    response.raise_for_status()
    return response.json()["access_token"]

def list_onedrive_root_files():
    access_token = get_access_token()

    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    user_id = "008ae000-382a-4483-b89b-19b2ff510bca"
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/drive/root/children"

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    return response.json()

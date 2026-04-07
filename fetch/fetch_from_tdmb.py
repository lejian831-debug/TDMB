import json
from pathlib import Path
API_KEY = None
READ_ACCESS_TOKEN = None

def load_api_key():
    CREDENTIALS_PATH = Path(__file__).parent.parent / "resource/api.json"

    with CREDENTIALS_PATH.open() as f:
        creds = json.load(f)

    API_KEY = creds["api_key"]
    READ_ACCESS_TOKEN = creds["read_access_token"]

    print(API_KEY, READ_ACCESS_TOKEN)  # just to verify


if __name__ == "__main__":

    load_api_key()
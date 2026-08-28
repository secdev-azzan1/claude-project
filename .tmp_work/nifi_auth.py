import os, requests, urllib3
urllib3.disable_warnings()
from dotenv import load_dotenv
from pathlib import Path
load_dotenv(Path(__file__).resolve().parents[1] / "backend" / ".env")

NIFI_URL = os.environ["NIFI_URL"].rstrip("/")
USER = os.environ["NIFI_USERNAME"]
PWD = os.environ["NIFI_PASSWORD"]

def get_token():
    r = requests.post(f"{NIFI_URL}/nifi-api/access/token", data={"username": USER, "password": PWD}, verify=False)
    r.raise_for_status()
    return r.text

def sess():
    s = requests.Session()
    s.verify = False
    s.headers["Authorization"] = f"Bearer {get_token()}"
    return s

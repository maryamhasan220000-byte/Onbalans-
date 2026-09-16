from datetime import datetime, timedelta, timezone 
import os
import requests
from dotenv import load_dotenv

load_dotenv()
token = os.getenv("ENTSOE_TOKEN")
url = "https://web-api.tp.entsoe.eu/api"
now = datetime.now(timezone.utc)
start = now.replace(hour=0, minute=0,microsecond=0)
end = start + timedelta(days=1)

params = {
    "securityToken": token,
    "documentType": "A44",
    "in_Domain": "10YNL----------L",
    "out_Domain": "10YNL----------L",
    "periodStart": start.strftime("%Y%m%d%H%M"),
    "periodEnd": end.strftime("%Y%m%d%H%M"),
    
}
r = requests.get(url, params=params, timeout=30)
print("status:", r.status_code)
print("URL:", r.url)
print(r.text[:2000])

with open("entsoe_sample.xml", "w", encoding="utf-8") as f:
    f.write(r.text)
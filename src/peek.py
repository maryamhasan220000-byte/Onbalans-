from datetime import datetime, timedelta, timezone
import os
import requests
from dotenv import load_dotenv
load_dotenv()
key = os.getenv("TENNET_API_KEY")
url = "https://api.tennet.eu/publications/v1/balance-delta-high-res"
headers = {
    "apikey": key,
    "Accept": "application/json"
}

now = datetime.now(timezone.utc)
thirty_minutes_ago = now - timedelta(minutes=30)

params = {
    "date_from": thirty_minutes_ago.strftime("%Y-%m-%dT%H:%M:%SZ"),
    "date_to": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
}

r = requests.get(
    url,
    headers=headers,
    params=params,
    timeout=30
)
data = r.json()

points = data["Response"]["TimeSeries"][0]["Period"][0]["points"]

latest = max(
    points,
    key=lambda x: x["timeInterval_start"]
)

latest_time = datetime.fromisoformat(
    latest["timeInterval_start"].replace("Z", "+00:00")
)

lag = now - latest_time

print("Current UTC:", now)
print("Newest TenneT measurement:", latest_time)
print("Lag:", lag)

print("Status:", r.status_code)
print("URL:", r.url)
print(r.text[:2000])

# producer.py
import time, uuid, random
import requests

REST_ENDPOINT = "http://localhost:8082"
CLUSTER_ID = "MkU3OEVBNTcwNTJENDM2Qg"
TOPIC = "events"

def maybe_none(value, probability=0.05):
    return None if random.random() < probability else value

def make_event():
    return {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "user_id": maybe_none(str(uuid.uuid4())),
        "event": maybe_none(random.choice(["click", "view", "purchase"])),
        "value": random.randint(1, 1500),
        "product_id": maybe_none(str(uuid.uuid4())),
        "category": maybe_none(random.choice(["electronics", "clothing", "books"])),
        "region": maybe_none(random.choice(["us-east-1", "us-west-2", "eu-central-1"])),
        "metadata": {"source": maybe_none(random.choice(["web", "mobile"]))},
    }

url = f"{REST_ENDPOINT}/v3/clusters/{CLUSTER_ID}/topics/{TOPIC}/records"

while True:
    payload = {"value": {"type": "JSON", "data": make_event()}}
    r = requests.post(url, json=payload)
    if r.status_code != 200:
        print(r.status_code, r.text)
    time.sleep(0.1)  # rate of record production 0.1 = ~10 events/sec
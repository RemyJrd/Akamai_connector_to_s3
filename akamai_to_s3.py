import requests
import json
import boto3
from datetime import datetime

def get_secrets(secret_name):
    client = boto3.client("secretsmanager", region_name="eu-west-1")
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])

secrets = get_secrets("akamai-api-secrets")
AKAMAI_API_KEY = secrets["AKAMAI_API_KEY"]
S3_BUCKET = secrets["S3_BUCKET"]

S3_CLIENT = boto3.client(
    "s3",
    aws_access_key_id=secrets.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=secrets.get("AWS_SECRET_ACCESS_KEY"),
) if "AWS_ACCESS_KEY_ID" in secrets else boto3.client("s3")

AKAMAI_API_URL = "https://api.akamai.com/siem/v1/events"
HEADERS = {
    "Authorization": f"Bearer {AKAMAI_API_KEY}",
    "Accept": "application/json"
}

def fetch_akamai_events():
    response = requests.get(AKAMAI_API_URL, headers=HEADERS)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erreur API Akamai: {response.status_code}")
        return None

def upload_to_s3(logs):
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    filename = f"akamai_logs_{timestamp}.json"
    s3_path = f"akamai_logs/{filename}"
    
    S3_CLIENT.put_object(
        Bucket=S3_BUCKET,
        Key=s3_path,
        Body=json.dumps(logs, indent=2),
        ContentType="application/json"
    )
    print(f"Logs sent to: s3://{S3_BUCKET}/{s3_path}")

def main():
    logs = fetch_akamai_events()
    if logs:
        upload_to_s3(logs)

if __name__ == "__main__":
    main()

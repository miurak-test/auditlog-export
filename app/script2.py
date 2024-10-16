import json
import os
import sys
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

# 環境変数からBigQueryの設定を取得
project_id = os.getenv('GCP_PROJECT_ID')
dataset_id = os.getenv('BQ_DATASET')
table_id = os.getenv('BQ_TABLE')
table_ref = f"{project_id}.{dataset_id}.{table_id}"

# BigQueryクライアントの初期化
client = bigquery.Client()

# 監査ログの読み込み
try:
    with open("app/audit_logs.json", "r") as f:
        logs = json.load(f)
except Exception as e:
    print(f"Error loading audit logs: {e}")
    sys.exit(1)

# データをBigQuery形式に変換
rows_to_insert = [
    {
        "timestamp": log.get("@timestamp") / 1000,
        "action": log.get("action"),
        "actor": log.get("actor"),
        "repository": log.get("repo"),
        "org": log.get("org"),
    }
    for log in logs if isinstance(log, dict)
]

# データをBigQueryに挿入
try:
    errors = client.insert_rows_json(table_ref, rows_to_insert)
    if errors:
        print(f"Errors occurred: {errors}")
    else:
        print("Data uploaded successfully.")
except GoogleAPIError as e:
    print(f"Failed to upload data to BigQuery: {e}")
    sys.exit(1)

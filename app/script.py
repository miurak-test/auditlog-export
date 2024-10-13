import json
import os
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# 環境変数から設定を取得
project_id = os.getenv('GCP_PROJECT_ID')
dataset_id = os.getenv('BQ_DATASET')
table_id = os.getenv('BQ_TABLE')
table_ref = f"{project_id}.{dataset_id}.{table_id}"

# BigQueryクライアントの初期化
client = bigquery.Client()

# テーブルの存在確認と作成
try:
    client.get_table(table_ref)
    print(f"Table {table_ref} already exists.")
except NotFound:
    print(f"Table {table_ref} not found. Creating a new one.")
    schema = [
        bigquery.SchemaField("timestamp", "TIMESTAMP"),
        bigquery.SchemaField("action", "STRING"),
        bigquery.SchemaField("actor", "STRING"),
        bigquery.SchemaField("repository", "STRING"),
        bigquery.SchemaField("org", "STRING"),
    ]
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table)
    print(f"Table {table_ref} created.")

# 監査ログの読み込み
with open("app/audit_logs.json", "r") as f:
    logs = json.load(f)

# データの整形とアップロード
rows_to_insert = [
    {
        "timestamp": log.get("@timestamp") / 1000,  # UNIXタイムを変換
        "action": log.get("action"),
        "actor": log.get("actor"),
        "repository": log.get("repo"),
        "org": log.get("org"),
    }
    for log in logs if isinstance(log, dict)
]

# データの挿入
if rows_to_insert:
    errors = client.insert_rows_json(table_ref, rows_to_insert)
    if errors:
        print(f"Errors occurred: {errors}")
    else:
        print("Data uploaded successfully")
else:
    print("No data to insert.")

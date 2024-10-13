import json
import os
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# BigQueryクライアントの初期化
client = bigquery.Client()

# 環境変数からプロジェクトID、データセット、テーブル名を取得
project_id = os.getenv("GCP_PROJECT_ID")
dataset_id = os.getenv("BQ_DATASET")
table_id = os.getenv("BQ_TABLE")
table_ref = f"{project_id}.{dataset_id}.{table_id}"

# スキーマの定義
schema = [
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("action", "STRING"),
    bigquery.SchemaField("actor", "STRING"),
    bigquery.SchemaField("repository", "STRING"),
    bigquery.SchemaField("org", "STRING"),
]

# テーブルが存在しない場合に作成
try:
    client.get_table(table_ref)
    print(f"Table {table_ref} already exists.")
except NotFound:
    print(f"Table {table_ref} not found. Creating a new one.")
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table)
    print(f"Table {table_ref} created.")

# `app`ディレクトリ内の`audit_logs.json`を読み込む
with open("app/audit_logs.json", "r") as f:
    logs = json.load(f)

# データの整形
rows_to_insert = [
    {
        "timestamp": log.get("@timestamp") / 1000,  # ミリ秒から秒に変換
        "action": log.get("action"),
        "actor": log.get("actor"),
        "repository": log.get("repo"),
        "org": log.get("org"),
    }
    for log in logs if isinstance(log, dict)
]

# データを挿入
if rows_to_insert:
    errors = client.insert_rows_json(table_ref, rows_to_insert)
    if errors:
        print("Errors occurred:", errors)
    else:
        print("Data uploaded successfully")
else:
    print("No data to insert.")

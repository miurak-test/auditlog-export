import json
import os
from google.cloud import bigquery
from google.api_core.exceptions import NotFound  # 修正ポイント

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

# データセットとテーブルの初期化
dataset_ref = client.dataset(dataset_id)
table = bigquery.Table(table_ref, schema=schema)

try:
    client.get_table(table_ref)  # テーブルの存在を確認
    print(f"Table {table_ref} already exists.")
except NotFound:  # 修正ポイント
    print(f"Table {table_ref} not found. Creating a new one.")
    client.create_table(table)  # テーブルを作成

# 監査ログを読み込む
with open("audit_logs.json", "r") as f:
    logs = json.load(f)

# データの整形
rows_to_insert = [
    {
        "timestamp": log.get("@timestamp"),
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

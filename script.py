import json
import os
from google.cloud import bigquery

# BigQueryクライアントの初期化
client = bigquery.Client()

# 環境変数からプロジェクトID、データセット、テーブル名を取得
project_id = os.getenv("GCP_PROJECT_ID")
dataset_id = os.getenv("BQ_DATASET")
table_id = os.getenv("BQ_TABLE")
table_ref = f"{project_id}.{dataset_id}.{table_id}"

# 監査ログをファイルから読み込み
with open("audit_logs.json", "r") as f:
    logs = json.load(f)

# データを整形してBigQueryに挿入する準備
rows_to_insert = [
    {
        "timestamp": log.get("@timestamp"),
        "action": log.get("action"),
        "actor": log.get("actor"),
        "repository": log.get("repo"),
        "org": log.get("org")
    }
    for log in logs if isinstance(log, dict)
]

# データが存在する場合のみBigQueryに挿入
if rows_to_insert:
    errors = client.insert_rows_json(table_ref, rows_to_insert)
    if errors:
        print("Errors occurred:", errors)
    else:
        print("Data uploaded successfully")
else:
    print("No data to insert.")

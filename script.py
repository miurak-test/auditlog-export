import json
import os
from google.cloud import bigquery

# BigQueryクライアントの設定
client = bigquery.Client()

# 環境変数からプロジェクトID、データセット、テーブルを取得
project_id = os.environ["GCP_PROJECT_ID"]
dataset_id = os.environ["BQ_DATASET"]
table_id = os.environ["BQ_TABLE"]
table_ref = f"{project_id}.{dataset_id}.{table_id}"

# 監査ログの読み込み
with open("audit_logs.json", "r") as f:
    logs = json.load(f)

# BigQueryに挿入するデータの整形
rows_to_insert = [
    {
        "timestamp": log["@timestamp"],
        "action": log["action"],
        "actor": log["actor"],
        "repository": log["repo"],
        "org": log["org"]
    }
    for log in logs
]

# データをBigQueryに挿入
errors = client.insert_rows_json(table_ref, rows_to_insert)
if errors:
    print("Errors occurred: ", errors)
else:
    print("Data uploaded successfully")

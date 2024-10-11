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
    try:
        logs = json.load(f)
        if isinstance(logs, str):
            logs = json.loads(logs)
    except json.JSONDecodeError as e:
        print(f"JSONのデコード中にエラーが発生しました: {e}")
        exit(1)

# デバッグのために監査ログの内容を表示
print("Fetched logs:", logs)

# BigQueryに挿入するデータの整形
rows_to_insert = [
    {
        "timestamp": log.get("@timestamp"),
        "action": log.get("action"),
        "actor": log.get("actor"),
        "repository": log.get("repo"),
        "org": log.get("org")
    }
    for log in logs
    if isinstance(log, dict)
]

# rows_to_insertが空でないか確認
if not rows_to_insert:
    print("挿入するデータがありません。")
    exit(0)

# データをBigQueryに挿入
errors = client.insert_rows_json(table_ref, rows_to_insert)
if errors:
    print("Errors occurred: ", errors)
else:
    print("Data uploaded successfully")

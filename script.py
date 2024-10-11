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
            # JSON文字列として返されている場合にデコード
            logs = json.loads(logs)
    except json.JSONDecodeError as e:
        print(f"JSONのデコード中にエラーが発生しました: {e}")
        exit(1)

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
    if isinstance(log, dict)  # 各要素が辞書型であることを確認
]

# データをBigQueryに挿入
errors = client.insert_rows_json(table_ref, rows_to_insert)
if errors:
    print("Errors occurred: ", errors)
else:
    print("Data uploaded successfully")

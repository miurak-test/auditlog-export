import os
import json
import jwt
import requests
from google.cloud import bigquery

def main():
    # 環境変数から秘密鍵を取得し、改行を復元
    private_key = os.getenv("APP_PRIVATE_KEY", "").replace("\\n", "\n")
    if not private_key:
        raise ValueError("APP_PRIVATE_KEY is not set or invalid.")

    # トークンを生成する処理を記述（例として）
    app_id = os.getenv("APP_ID")
    payload = {"iss": app_id, "exp": 3600}
    jwt_token = jwt.encode(payload, private_key, algorithm="RS256")

    print(f"Generated JWT token: {jwt_token}")

    # BigQueryクライアントの初期化
    client = bigquery.Client()
    table_ref = f"{os.getenv('GCP_PROJECT_ID')}.{os.getenv('BQ_DATASET')}.{os.getenv('BQ_TABLE')}"

    # 監査ログのアップロード
    with open("app/audit_logs.json", "r") as f:
        logs = json.load(f)
    rows_to_insert = [{"json": log} for log in logs]

    errors = client.insert_rows_json(table_ref, rows_to_insert)
    if errors:
        print(f"Errors occurred: {errors}")
    else:
        print("Data uploaded successfully.")

if __name__ == "__main__":
    main()

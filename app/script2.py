import os
import jwt
import time
import requests
import json
import logging
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, GoogleAPIError

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# 環境変数から設定を取得
project_id = os.getenv("GCP_PROJECT_ID")
dataset_id = os.getenv("BQ_DATASET")
table_id = os.getenv("BQ_TABLE")
table_ref = f"{project_id}.{dataset_id}.{table_id}"

app_id = os.getenv("APP_ID")
installation_id = os.getenv("APP_INSTALLATION_ID")
private_key = os.getenv("APP_PRIVATE_KEY").encode()

def get_bigquery_client():
    """BigQueryクライアントを初期化して返す"""
    return bigquery.Client()

def generate_jwt(app_id, private_key):
    """JWTを生成して返す"""
    payload = {
        "iat": int(time.time()),  
        "exp": int(time.time()) + (10 * 60),  
        "iss": app_id  
    }
    return jwt.encode(payload, private_key, algorithm="RS256")

def get_access_token(installation_id, jwt_token):
    """JWTを使ってGitHubのアクセストークンを取得"""
    url = f"https://api.github.com/app/installations/{installation_id}/access_tokens"
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/vnd.github+json"
    }
    response = requests.post(url, headers=headers)
    response.raise_for_status()
    return response.json()["token"]

def fetch_audit_logs(access_token):
    """監査ログを取得"""
    url = "https://api.github.com/orgs/miurak-test/audit-log"
    headers = {
        "Authorization": f"token {access_token}",
        "Accept": "application/vnd.github+json"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    logs = response.json()
    logging.info(f"Fetched {len(logs)} logs.")
    return logs

def save_logs_to_file(logs, file_path="app/audit_logs.json"):
    """監査ログをJSONファイルに保存"""
    with open(file_path, "w") as f:
        json.dump(logs, f, indent=4)
    logging.info(f"Logs saved to {file_path}.")

def create_table_if_not_exists(client, table_ref):
    """BigQueryのテーブルが存在するか確認し、無ければ作成"""
    try:
        client.get_table(table_ref)
        logging.info(f"Table {table_ref} already exists.")
    except NotFound:
        logging.warning(f"Table {table_ref} not found. Creating a new one.")
        schema = [
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("action", "STRING"),
            bigquery.SchemaField("actor", "STRING"),
            bigquery.SchemaField("repository", "STRING"),
            bigquery.SchemaField("org", "STRING"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        logging.info(f"Table {table_ref} created.")

def transform_logs(logs):
    """監査ログをBigQuery用の形式に変換"""
    return [
        {
            "timestamp": log.get("@timestamp") / 1000,
            "action": log.get("action"),
            "actor": log.get("actor"),
            "repository": log.get("repo"),
            "org": log.get("org"),
        }
        for log in logs if isinstance(log, dict)
    ]

def insert_rows_with_retry(client, table_ref, rows, retries=5, delay=10):
    """データをBigQueryに挿入（リトライ付き）"""
    for attempt in range(retries):
        try:
            logging.info(f"Inserting rows into {table_ref} (Attempt {attempt + 1}/{retries})...")
            errors = client.insert_rows_json(table_ref, rows)
            if not errors:
                logging.info("Data uploaded successfully.")
                return
            else:
                logging.error(f"Errors occurred: {errors}")
                time.sleep(delay)
        except GoogleAPIError as e:
            logging.error(f"Failed to upload data to BigQuery: {e}")
            time.sleep(delay)
    logging.error(f"Failed to insert rows into {table_ref} after {retries} attempts.")
    raise RuntimeError("BigQuery insertion failed.")

def main():
    """メイン処理"""
    try:
        # JWT生成とアクセストークン取得
        jwt_token = generate_jwt(app_id, private_key)
        access_token = get_access_token(installation_id, jwt_token)

        # 監査ログを取得して保存
        logs = fetch_audit_logs(access_token)
        save_logs_to_file(logs)

        # BigQueryにデータを挿入
        client = get_bigquery_client()
        create_table_if_not_exists(client, table_ref)
        rows_to_insert = transform_logs(logs)
        insert_rows_with_retry(client, table_ref, rows_to_insert)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

if __name__ == "__main__":
    main()

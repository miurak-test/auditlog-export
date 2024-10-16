import os
import json
import time
import logging
import requests
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, GoogleAPIError

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def get_bigquery_client():
    """BigQueryクライアントを初期化"""
    return bigquery.Client()

def create_table_if_not_exists(client, table_ref):
    """テーブルが存在しない場合に作成"""
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
        wait_for_table(client, table_ref)

def wait_for_table(client, table_ref, retries=5, delay=10):
    """テーブルが利用可能になるまで待機"""
    for attempt in range(retries):
        try:
            logging.info(f"Checking if table {table_ref} is available (Attempt {attempt + 1}/{retries})...")
            client.get_table(table_ref)
            logging.info(f"Table {table_ref} is now available.")
            return
        except NotFound:
            logging.warning(f"Table {table_ref} not available yet. Waiting {delay} seconds...")
            time.sleep(delay)
    logging.error(f"Table {table_ref} is not available after {retries} attempts.")
    exit(1)

def generate_access_token(app_id, private_key, installation_id):
    """JWTを使用してGitHub AppのAccess Tokenを取得"""
    jwt_token = os.popen(f'python3 app/generate_jwt.py').read().strip()
    
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/vnd.github.v3+json"
    }

    url = f"https://api.github.com/app/installations/{installation_id}/access_tokens"
    response = requests.post(url, headers=headers)

    if response.status_code == 201:
        token = response.json()["token"]
        logging.info("Successfully generated access token.")
        return token
    else:
        logging.error(f"Failed to generate access token: {response.text}")
        exit(1)

def fetch_audit_logs(access_token, last_run):
    """GitHubから監査ログを取得"""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/vnd.github.v3+json"
    }

    url = f"https://api.github.com/orgs/miurak-test/audit-log?after={last_run}"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        logs = response.json()
        logging.info(f"Fetched {len(logs)} logs from GitHub.")
        return logs
    else:
        logging.error(f"Failed to fetch audit logs: {response.text}")
        exit(1)

def transform_logs(logs):
    """監査ログをBigQueryの形式に変換"""
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
    """BigQueryにリトライ付きでデータを挿入"""
    for attempt in range(retries):
        try:
            logging.info(f"Inserting rows into {table_ref} (Attempt {attempt + 1}/{retries})...")
            errors = client.insert_rows_json(table_ref, rows)
            if errors:
                logging.error(f"Errors during insertion: {errors}")
                time.sleep(delay)
            else:
                logging.info("Data uploaded successfully.")
                return
        except GoogleAPIError as e:
            logging.error(f"Failed to upload data to BigQuery: {e}")
            time.sleep(delay)
    logging.error(f"Failed to insert rows into {table_ref} after {retries} attempts.")
    exit(1)

def main():
    """メイン処理"""
    # 環境変数から必要な情報を取得
    app_id = os.getenv("APP_ID")
    installation_id = os.getenv("APP_INSTALLATION_ID")
    private_key = os.getenv("APP_PRIVATE_KEY").replace("\\n", "\n")

    # BigQueryの設定
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = os.getenv("BQ_DATASET")
    table_id = os.getenv("BQ_TABLE")
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # BigQueryクライアントを初期化
    client = get_bigquery_client()
    create_table_if_not_exists(client, table_ref)

    # GitHub AppのAccess Tokenを取得
    access_token = generate_access_token(app_id, private_key, installation_id)

    # 前回の実行時刻を取得
    with open("artifacts/last_run_timestamp.txt", "r") as f:
        last_run = f.read().strip()

    # GitHubから監査ログを取得
    logs = fetch_audit_logs(access_token, last_run)

    # 取得した監査ログを変換
    rows_to_insert = transform_logs(logs)

    # BigQueryにデータを挿入
    insert_rows_with_retry(client, table_ref, rows_to_insert)

if __name__ == "__main__":
    main()

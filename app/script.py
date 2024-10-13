import json
import logging
import os
import sys
import time
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, GoogleAPIError

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def get_bigquery_client():
    """BigQueryクライアントを初期化して返す"""
    return bigquery.Client()

def create_table_if_not_exists(client, table_ref):
    """テーブルの存在確認と、存在しない場合の作成"""
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

def wait_for_table(client, table_ref, retries=5, delay=30):
    """テーブルが使用可能になるまで待機し、リトライする"""
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
    sys.exit(1)

def load_logs(file_path):
    """監査ログの読み込み"""
    try:
        with open(file_path, "r") as f:
            logs = json.load(f)
        logging.info(f"Loaded {len(logs)} logs from {file_path}.")
        return logs
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"Failed to load logs: {e}")
        sys.exit(1)

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
    """BigQueryにデータをリトライ付きで挿入"""
    for attempt in range(retries):
        try:
            logging.info(f"Inserting rows into {table_ref} (Attempt {attempt + 1}/{retries})...")
            errors = client.insert_rows_json(table_ref, rows)
            if errors:
                logging.error(f"Errors occurred during insertion: {errors}")
                time.sleep(delay)
            else:
                logging.info("Data uploaded successfully.")
                return
        except GoogleAPIError as e:
            logging.error(f"Failed to upload data to BigQuery: {e}")
            time.sleep(delay)
    logging.error(f"Failed to insert rows into {table_ref} after {retries} attempts.")
    sys.exit(1)

def main():
    """メイン処理"""
    project_id = os.getenv('GCP_PROJECT_ID')
    dataset_id = os.getenv('BQ_DATASET')
    table_id = os.getenv('BQ_TABLE')
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    client = get_bigquery_client()
    create_table_if_not_exists(client, table_ref)

    logs = load_logs("app/audit_logs.json")
    rows_to_insert = transform_logs(logs)

    insert_rows_with_retry(client, table_ref, rows_to_insert)

if __name__ == "__main__":
    main()

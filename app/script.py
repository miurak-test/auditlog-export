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

        # テーブル作成直後のAPI遅延対策で30秒待機
        logging.info("Waiting for 30 seconds to ensure the table is ready...")
        time.sleep(30)

def load_logs(file_path):
    """監査ログの読み込み"""
    try:
        with open(file_path, "r") as f:
            logs = json.load(f)
        logging.info(f"Loaded {len(logs)} logs from {file_path}.")
        return logs
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"Failed to load logs: {e}")
        sys.exit(1)  # エラー時に終了

def transform_logs(logs):
    """監査ログをBigQuery用の形式に変換"""
    return [
        {
            "timestamp": log.get("@timestamp") / 1000,  # UNIXタイムを秒に変換
            "action": log.get("action"),
            "actor": log.get("actor"),
            "repository": log.get("repo"),
            "org": log.get("org"),
        }
        for log in logs if isinstance(log, dict)
    ]

def insert_rows_to_bigquery(client, table_ref, rows):
    """BigQueryにデータを挿入"""
    if not rows:
        logging.info("No data to insert.")
        return

    try:
        errors = client.insert_rows_json(table_ref, rows)
        if errors:
            logging.error(f"Errors occurred during insertion: {errors}")
            sys.exit(1)  # エラー時に終了
        else:
            logging.info("Data uploaded successfully.")
    except GoogleAPIError as e:
        logging.error(f"Failed to upload data to BigQuery: {e}")
        sys.exit(1)  # エラー時に終了

def main():
    """メイン処理"""
    project_id = os.getenv('GCP_PROJECT_ID')
    dataset_id = os.getenv('BQ_DATASET')
    table_id = os.getenv('BQ_TABLE')
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    client = get_bigquery_client()

    # テーブルの存在確認と作成
    create_table_if_not_exists(client, table_ref)

    # 監査ログの読み込みと変換
    logs = load_logs("app/audit_logs.json")
    rows_to_insert = transform_logs(logs)

    # BigQueryにデータを挿入
    insert_rows_to_bigquery(client, table_ref, rows_to_insert)

if __name__ == "__main__":
    main()

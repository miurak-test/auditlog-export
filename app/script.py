import json
import os
import logging
import sys
import time  # 追加
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, GoogleAPIError

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def get_bigquery_client():
    """BigQueryクライアントを初期化して返す"""
    return bigquery.Client()

def create_table_if_not_exists(client, table_ref):
    """
    テーブルが存在しない場合、新規作成
    """
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

def load_logs():
    """複数ファイルから監査ログを読み込み、1つのリストに統合"""
    logs = []
    logs_dir = 'app/audit_logs'
    
    for filename in os.listdir(logs_dir):
        if filename.endswith(".json"):
            file_path = os.path.join(logs_dir, filename)
            logging.info(f"Loading logs from {file_path}")
            with open(file_path, 'r') as f:
                try:
                    file_logs = json.load(f)
                    logs.extend(file_logs)
                except json.JSONDecodeError as e:
                    logging.error(f"Failed to load {file_path}: {e}")
                    sys.exit(1)
    
    logging.info(f"Total logs loaded: {len(logs)}")
    return logs

def transform_logs(logs):
    """監査ログをBigQuery用の形式に変換"""
    transformed_logs = []
    
    for log in logs:
        transformed_log = {
            "timestamp": log.get("@timestamp") / 1000 if log.get("@timestamp") else None,
            "action": log.get("action"),
            "actor": log.get("actor"),
            "repository": log.get("repo"),
            "org": log.get("org")
        }
        transformed_logs.append(transformed_log)
    
    return transformed_logs

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
    project_id = os.getenv('GCP_PROJECT_ID')
    dataset_id = os.getenv('BQ_DATASET')
    table_id = os.getenv('BQ_TABLE')
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    client = get_bigquery_client()

    create_table_if_not_exists(client, table_ref)  # テーブルが存在しない場合作成

    logs = load_logs()  # 取得した監査ログを読み込み
    rows_to_insert = transform_logs(logs)  # BigQuery用に変換

    insert_rows_with_retry(client, table_ref, rows_to_insert)

if __name__ == "__main__":
    main()

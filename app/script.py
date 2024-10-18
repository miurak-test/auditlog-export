import json
import os
import logging
import sys
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, GoogleAPIError

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def get_bigquery_client():
    """BigQueryクライアントを初期化して返す"""
    return bigquery.Client()

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
    """
    監査ログをBigQuery用の形式に変換
    - 必要なフィールドのみ抽出し、整形する
    """
    transformed_logs = []
    
    for log in logs:
        transformed_log = {
            "timestamp": log.get("@timestamp") / 1000 if log.get("@timestamp") else None,  # UNIXタイムを秒に変換
            "action": log.get("action"),  # アクション名
            "actor": log.get("actor"),  # 実行者
            "repository": log.get("repo"),  # リポジトリ名
            "org": log.get("org")  # 組織名
        }
        transformed_logs.append(transformed_log)
    
    return transformed_logs

def insert_rows_with_retry(client, table_ref, rows, retries=5, delay=10):
    """
    BigQueryにデータをリトライ付きで挿入
    - 挿入が失敗した場合、一定時間待機して再試行
    """
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

    logs = load_logs()  # 取得した監査ログを読み込み
    rows_to_insert = transform_logs(logs)  # BigQuery用に変換

    insert_rows_with_retry(client, table_ref, rows_to_insert)

if __name__ == "__main__":
    main()

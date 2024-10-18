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

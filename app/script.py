import json
import os
import logging
import sys
import time  # 追加: リトライ時に待機するためのtimeモジュール
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, GoogleAPIError

# ロギング設定: 実行中のメッセージをINFOレベルで出力する
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def get_bigquery_client():
    """BigQueryクライアントを初期化して返す"""
    return bigquery.Client()

def create_table_if_not_exists(client, table_ref):
    """
    テーブルが存在しない場合、新規作成
    - BigQueryのテーブルが存在しないときに作成する処理
    """
    try:
        client.get_table(table_ref)  # テーブルが存在するか確認
        logging.info(f"Table {table_ref} already exists.")  # テーブルが存在する場合のログ
    except NotFound:
        logging.warning(f"Table {table_ref} not found. Creating a new one.")  # テーブルが見つからない場合に新規作成のログを出力
        # テーブルのスキーマを定義
        schema = [
            bigquery.SchemaField("timestamp", "TIMESTAMP"),  # タイムスタンプフィールド
            bigquery.SchemaField("action", "STRING"),  # アクションフィールド
            bigquery.SchemaField("actor", "STRING"),  # アクター（実行者）フィールド
            bigquery.SchemaField("repository", "STRING"),  # リポジトリ名フィールド
            bigquery.SchemaField("org", "STRING"),  # 組織名フィールド
        ]
        table = bigquery.Table(table_ref, schema=schema)  # 新しいテーブルを定義
        client.create_table(table)  # テーブルを作成
        logging.info(f"Table {table_ref} created.")  # 作成完了のログを出力

def load_logs():
    """
    複数ファイルから監査ログを読み込み、1つのリストに統合
    - 監査ログを保存しているJSONファイルを読み込み、1つのリストにまとめる
    """
    logs = []
    logs_dir = 'app/audit_logs'  # ログファイルが保存されているディレクトリのパス

    # ディレクトリ内のすべてのJSONファイルを処理
    for filename in os.listdir(logs_dir):
        if filename.endswith(".json"):  # .jsonファイルのみ対象
            file_path = os.path.join(logs_dir, filename)
            logging.info(f"Loading logs from {file_path}")  # 読み込むファイルをログに出力
            with open(file_path, 'r') as f:
                try:
                    file_logs = json.load(f)  # ファイルの内容を読み込んでログとして保存
                    logs.extend(file_logs)  # 全ログリストに追加
                except json.JSONDecodeError as e:  # 読み込みエラーの処理
                    logging.error(f"Failed to load {file_path}: {e}")
                    sys.exit(1)  # エラーがあれば終了
    
    logging.info(f"Total logs loaded: {len(logs)}")  # 読み込んだログの総数をログ出力
    return logs

def transform_logs(logs):
    """
    監査ログをBigQuery用の形式に変換
    - 読み込んだ監査ログをBigQueryに挿入可能な形式に整形
    """
    transformed_logs = []

    # 各ログの項目を整形してリストに追加
    for log in logs:
        transformed_log = {
            "timestamp": log.get("@timestamp") / 1000 if log.get("@timestamp") else None,  # タイムスタンプを秒に変換
            "action": log.get("action"),  # アクション名
            "actor": log.get("actor"),  # 実行者
            "repository": log.get("repo"),  # リポジトリ名
            "org": log.get("org")  # 組織名
        }
        transformed_logs.append(transformed_log)  # 整形したログを追加
    
    return transformed_logs

def insert_rows_with_retry(client, table_ref, rows, retries=5, delay=10):
    """
    BigQueryにデータをリトライ付きで挿入
    - エラーが発生した場合、リトライを最大5回実行
    """
    for attempt in range(retries):
        try:
            logging.info(f"Inserting rows into {table_ref} (Attempt {attempt + 1}/{retries})...")  # 挿入処理の試行回数をログ出力
            errors = client.insert_rows_json(table_ref, rows)  # データをBigQueryに挿入
            if errors:  # エラーが発生した場合
                logging.error(f"Errors occurred during insertion: {errors}")
                time.sleep(delay)  # 指定した秒数待機してリトライ
            else:
                logging.info("Data uploaded successfully.")  # 挿入成功の場合
                return  # 処理終了
        except GoogleAPIError as e:  # APIエラー処理
            logging.error(f"Failed to upload data to BigQuery: {e}")
            time.sleep(delay)  # 指定した秒数待機してリトライ
    logging.error(f"Failed to insert rows into {table_ref} after {retries} attempts.")  # リトライ最大回数に達した場合のログ出力
    sys.exit(1)

def main():
    """メイン処理"""
    project_id = os.getenv('GCP_PROJECT_ID')  # 環境変数からGCPプロジェクトIDを取得
    dataset_id = os.getenv('BQ_DATASET')  # 環境変数からBigQueryデータセットIDを取得
    table_id = os.getenv('BQ_TABLE')  # 環境変数からBigQueryテーブルIDを取得
    table_ref = f"{project_id}.{dataset_id}.{table_id}"  # テーブルの参照を作成

    client = get_bigquery_client()  # BigQueryクライアントを初期化

    create_table_if_not_exists(client, table_ref)  # テーブルが存在しない場合に作成

    logs = load_logs()  # 監査ログをファイルから読み込む
    rows_to_insert = transform_logs(logs)  # 読み込んだログをBigQuery用に整形

    insert_rows_with_retry(client, table_ref, rows_to_insert)  # BigQueryにデータを挿入

if __name__ == "__main__":
    main()  # スクリプトを実行

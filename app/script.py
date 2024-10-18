import json  # JSON形式のデータの操作に使用
import logging  # ログの出力に使用
import os  # 環境変数の取得やファイル操作に使用
import sys  # スクリプトの終了処理に使用
import time  # 一定時間の待機に使用
from google.cloud import bigquery  # BigQueryクライアントを提供するライブラリ
from google.api_core.exceptions import NotFound, GoogleAPIError  # BigQueryエラー処理用

# ログ設定：実行中のメッセージをINFOレベルで出力する
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def get_bigquery_client():
    """BigQueryクライアントを初期化して返す"""
    return bigquery.Client()  # デフォルト認証情報を使用してクライアントを作成

def create_table_if_not_exists(client, table_ref):
    """
    テーブルの存在確認と、存在しない場合の作成処理
    - テーブルが存在しない場合、新規作成する
    """
    try:
        client.get_table(table_ref)  # テーブルの存在確認
        logging.info(f"Table {table_ref} already exists.")  # テーブルが存在する場合のログ
    except NotFound:  # テーブルが存在しない場合の処理
        logging.warning(f"Table {table_ref} not found. Creating a new one.")  # 警告ログを出力
        schema = [
            bigquery.SchemaField("timestamp", "TIMESTAMP"),  # タイムスタンプのフィールド
            bigquery.SchemaField("action", "STRING"),  # アクションの名前
            bigquery.SchemaField("actor", "STRING"),  # アクションを実行したユーザー
            bigquery.SchemaField("repository", "STRING"),  # 関連するリポジトリ名
            bigquery.SchemaField("org", "STRING"),  # 関連する組織名
        ]
        table = bigquery.Table(table_ref, schema=schema)  # 新しいテーブルの定義
        client.create_table(table)  # テーブルの作成
        logging.info(f"Table {table_ref} created.")  # 作成成功のログを出力
        wait_for_table(client, table_ref)  # テーブルの反映待機

def wait_for_table(client, table_ref, retries=5, delay=30):
    """
    テーブルが利用可能になるまで待機し、リトライする
    - 利用可能になるまで最大5回リトライ
    """
    for attempt in range(retries):  # 指定した回数までリトライ
        try:
            logging.info(f"Checking if table {table_ref} is available (Attempt {attempt + 1}/{retries})...")
            client.get_table(table_ref)  # テーブルが存在するか確認
            logging.info(f"Table {table_ref} is now available.")  # 利用可能のログ
            return  # テーブルが見つかった場合、処理終了
        except NotFound:  # テーブルがまだ利用できない場合
            logging.warning(f"Table {table_ref} not available yet. Waiting {delay} seconds...")
            time.sleep(delay)  # 一定時間待機して再試行
    logging.error(f"Table {table_ref} is not available after {retries} attempts.")  # 失敗時のログ
    sys.exit(1)  # エラー終了

def load_logs(file_path):
    """
    監査ログの読み込み
    - 指定されたパスからJSONファイルを読み込む
    """
    try:
        with open(file_path, "r") as f:  # ファイルを読み込みモードで開く
            logs = [json.loads(line) for line in f]  # JSONデータを読み込む
        logging.info(f"Loaded {len(logs)} logs from {file_path}.")  # 読み込んだ件数をログ出力
        return logs  # 読み込んだデータを返す
    except (FileNotFoundError, json.JSONDecodeError) as e:  # ファイルがない、またはJSONエラーの場合
        logging.error(f"Failed to load logs: {e}")  # エラーログを出力
        sys.exit(1)  # エラー終了

def transform_logs(logs):
    """
    監査ログをBigQuery用の形式に変換
    - 必要なフィールドのみ抽出し、整形する
    """
    return [
        {
            "timestamp": log.get("@timestamp") / 1000,  # UNIXタイムを秒単位に変換
            "action": log.get("action"),  # アクション名
            "actor": log.get("actor"),  # 実行者
            "repository": log.get("repo"),  # リポジトリ名
            "org": log.get("org"),  # 組織名
        }
        for log in logs if isinstance(log, dict)  # 辞書形式のログのみ処理
    ]

def insert_rows_with_retry(client, table_ref, rows, retries=5, delay=10):
    """
    BigQueryにデータをリトライ付きで挿入
    - 挿入が失敗した場合、一定時間待機して再試行
    """
    for attempt in range(retries):  # 指定回数までリトライ
        try:
            logging.info(f"Inserting rows into {table_ref} (Attempt {attempt + 1}/{retries})...")
            errors = client.insert_rows_json(table_ref, rows)  # データの挿入
            if errors:  # 挿入時にエラーが発生した場合
                logging.error(f"Errors occurred during insertion: {errors}")
                time.sleep(delay)  # 再試行まで待機
            else:
                logging.info("Data uploaded successfully.")  # 成功のログ
                return  # 挿入成功の場合、処理終了
        except GoogleAPIError as e:  # APIエラーが発生した場合
            logging.error(f"Failed to upload data to BigQuery: {e}")
            time.sleep(delay)  # 再試行まで待機
    logging.error(f"Failed to insert rows into {table_ref} after {retries} attempts.")  # 失敗ログ
    sys.exit(1)  # エラー終了

def main():
    """メイン処理"""
    project_id = os.getenv('GCP_PROJECT_ID')  # GCPプロジェクトIDを取得
    dataset_id = os.getenv('BQ_DATASET')  # BigQueryデータセットIDを取得
    table_id = os.getenv('BQ_TABLE')  # テーブルIDを取得
    table_ref = f"{project_id}.{dataset_id}.{table_id}"  # テーブルの完全名を構築

    client = get_bigquery_client()  # BigQueryクライアントを初期化
    create_table_if_not_exists(client, table_ref)  # テーブルの存在確認と作成

    logs = load_logs("app/audit_logs.json")  # 監査ログを読み込み
    rows_to_insert = transform_logs(logs)  # ログをBigQuery用に変換

    insert_rows_with_retry(client, table_ref, rows_to_insert)  # データを挿入

if __name__ == "__main__":
    main()  # スクリプトのエントリーポイント

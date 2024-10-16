terraform {
  # GCS（Google Cloud Storage）をバックエンドとして使用
  backend "gcs" {
    # Terraformの状態ファイル（state file）を保存するGCSバケットの名前
    bucket = "miurak_terraform_dev2_tfstate"

    # GCSバケット内のディレクトリ構造に対するプレフィックス（Terraform状態ファイルが保存される場所）
    prefix = "terraform/github-audit-logs-export/state"
  }
}

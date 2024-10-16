locals {
  github_repository = "miurak-test/auditlog-export"
  project_id        = "miurak"
  region            = "asia-northeast1"
  account_id        = "git-audit-gha-sa1"
  pool_id           = "gha-pool-git-export1"
  provider_id       = "github"

  # データセット名の指定
  bigquery_dataset_id = "github_audit_logs"

  services = toset([
    "iam.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "iamcredentials.googleapis.com",
    "sts.googleapis.com",
    "bigquery.googleapis.com"
  ])

  roles = toset([
    "roles/serviceusage.serviceUsageViewer",
    "roles/iam.serviceAccountTokenCreator",
    "roles/iam.serviceAccountOpenIdTokenCreator",
    "roles/bigquery.dataEditor", # BigQuery テーブルへのデータ書き込み
    "roles/bigquery.jobUser"     # BigQuery ジョブの実行（クエリやロードジョブ）
  ])

  sa_roles = flatten([
    for role in local.roles : {
      sa_id = "${local.account_id}@${local.project_id}.iam.gserviceaccount.com"
      role  = role
    }
  ])
}

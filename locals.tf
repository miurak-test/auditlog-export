locals {
  github_repository                  = "miurak-test/auditlog-export"
  project_id                         = "miurak"
  region                             = "asia-northeast1"
  account_id                         = "git-audit-gha-sa"
  terraform_service_account          = "${local.account_id}@miurak.iam.gserviceaccount.com"
  workload_identity_pool_id          = "gha-pool-git-export"
  workload_identity_pool_provider_id = "github"

  # API 有効化用
  services = toset([                       # Workload Identity 連携用
    "iam.googleapis.com",                  # IAM
    "cloudresourcemanager.googleapis.com", # Resource Manager
    "iamcredentials.googleapis.com",       # Service Account Credentials
    "sts.googleapis.com"                   # Security Token Service API
  ])

  # 付与するロールのリストを作成
  roles = toset([
    "roles/serviceusage.serviceUsageViewer",
    "roles/iam.serviceAccountTokenCreator",
    "roles/iam.serviceAccountOpenIdTokenCreator",
    "roles/editor"
  ])

  sa_roles = flatten([
    for role in local.roles : {
      sa_id = local.terraform_service_account
      role  = role
    }
  ])
}

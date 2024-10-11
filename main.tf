## API の有効化 (Workload Identity 用)
resource "google_project_service" "enable_api" {
  for_each                   = local.services
  project                    = local.project_id
  service                    = each.value
  disable_dependent_services = true
}

# サービスアカウントの作成
resource "google_service_account" "terraform_sa" {
  account_id   = local.account_id
  display_name = "Terraform GHA Service Account"
  project      = local.project_id
}

# サービスアカウントとロールの紐づけ設定
resource "google_project_iam_member" "this" {
  for_each = {
    for sa_role in local.sa_roles :
    "${sa_role.sa_id}-${sa_role.role}" => sa_role
  }

  project = local.project_id
  role    = each.value.role
  member  = "serviceAccount:${google_service_account.terraform_sa.email}"
}

# Workload Identity Pool 設定
resource "google_iam_workload_identity_pool" "mypool" {
  project                   = local.project_id
  workload_identity_pool_id = local.workload_identity_pool_id
  display_name              = local.workload_identity_pool_id
  description               = "GitHub Actions で使用"
}

# Workload Identity Provider 設定
resource "google_iam_workload_identity_pool_provider" "myprovider" {
  depends_on = [
    google_iam_workload_identity_pool.mypool,
  ]
  project                            = local.project_id
  workload_identity_pool_id          = google_iam_workload_identity_pool.mypool.workload_identity_pool_id
  workload_identity_pool_provider_id = local.workload_identity_pool_provider_id
  display_name                       = local.workload_identity_pool_provider_id
  description                        = "GitHub Actions で使用"

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
  }

  attribute_condition = "assertion.repository == '${local.github_repository}'"

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

# サービスアカウントの IAM Policy 設定と GitHub リポジトリの指定
resource "google_service_account_iam_member" "terraform_sa" {
  depends_on = [
    google_iam_workload_identity_pool_provider.myprovider,
  ]
  service_account_id = google_service_account.terraform_sa.id
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.mypool.name}/attribute.repository/${local.github_repository}"
}

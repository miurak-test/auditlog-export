resource "google_iam_workload_identity_pool" "default" {
  project                   = var.project_id
  workload_identity_pool_id = var.pool_id
  display_name              = var.pool_id
  description               = "GitHub Actions で使用"
}

resource "google_iam_workload_identity_pool_provider" "default" {
  depends_on = [google_iam_workload_identity_pool.default]

  project                            = var.project_id
  workload_identity_pool_id          = google_iam_workload_identity_pool.default.workload_identity_pool_id
  workload_identity_pool_provider_id = var.provider_id
  display_name                       = var.provider_id
  description                        = "GitHub Actions で使用"

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
  }

  attribute_condition = "assertion.repository == '${var.github_repository}'"

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

resource "google_service_account_iam_member" "default" {
  depends_on = [google_iam_workload_identity_pool_provider.default]

  service_account_id = var.service_account_id
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.default.name}/attribute.repository/${var.github_repository}"
}

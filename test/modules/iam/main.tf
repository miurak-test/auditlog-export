resource "google_service_account" "default" {
  account_id   = var.account_id
  display_name = "Terraform GHA Service Account"
  project      = var.project_id
}

resource "google_project_iam_member" "default" {
  for_each = {
    for sa_role in var.sa_roles :
    "${sa_role.sa_id}-${sa_role.role}" => sa_role
  }

  project = var.project_id
  role    = each.value.role
  member  = "serviceAccount:${google_service_account.default.email}"
}

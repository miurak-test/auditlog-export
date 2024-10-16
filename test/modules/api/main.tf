resource "google_project_service" "default" {
  for_each                   = toset(var.services)
  project                    = var.project_id
  service                    = each.value
  disable_dependent_services = true
}

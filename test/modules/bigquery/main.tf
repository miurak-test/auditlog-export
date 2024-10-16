resource "google_bigquery_dataset" "default" {
  dataset_id  = var.dataset_id
  project     = var.project_id
  location    = var.location
  description = "BigQuery dataset"
}

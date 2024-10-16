variable "dataset_id" {
  type        = string
  description = "The ID of the BigQuery dataset"
}

variable "project_id" {
  type        = string
  description = "The ID of the GCP project"
}

variable "location" {
  type        = string
  default     = "asia-northeast1"
  description = "The location for the BigQuery dataset"
}

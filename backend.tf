terraform {
  backend "gcs" {
    bucket = "miurak-tfstate-gitexport"
    prefix = "terraform/state"
  }
}

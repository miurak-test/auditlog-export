module "api" {
  source     = "../../modules/api"
  services   = local.services
  project_id = local.project_id
}

module "iam" {
  source     = "../../modules/iam"
  account_id = local.account_id
  project_id = local.project_id
  sa_roles   = local.sa_roles
}

module "workload_identity" {
  source             = "../../modules/workload_identity"
  project_id         = local.project_id
  pool_id            = local.pool_id
  provider_id        = local.provider_id
  github_repository  = local.github_repository
  service_account_id = module.iam.service_account_id
}

module "bigquery" {
  source     = "../../modules/bigquery"
  project_id = local.project_id
  dataset_id = local.bigquery_dataset_id
  location   = local.region
}

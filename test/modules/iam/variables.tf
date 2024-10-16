variable "account_id" {
  type = string
}

variable "project_id" {
  type = string
}

variable "sa_roles" {
  type = list(object({
    sa_id = string
    role  = string
  }))
}

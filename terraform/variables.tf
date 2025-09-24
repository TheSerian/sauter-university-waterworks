variable "project" {
  description = "ID do projeto GCP onde os recursos serão criados"
  type        = string
}

variable "region" {
  default = "us-central1"
}

variable "zone" {
  default = "us-central1-c"
}

variable "github_repository_name" {
  description = "Nome do repostório no Github, usado para restringir o WIF"
  type        = string
}
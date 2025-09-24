resource "google_iam_workload_identity_pool" "github_pool" {
  workload_identity_pool_id = "github-actions-pool"

  project = var.project

  display_name = "WIP para Github Actions"
  description  = "Contêiner para federação de identidades do Github"
  disabled     = false
}

resource "google_iam_workload_identity_pool_provider" "github_provider" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github_pool.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-oidc-provider"

  project = var.project

  display_name = "Provedor GitHub OIDC"
  description  = "Configuração de confiança para tokens OIDC do GitHub."

  attribute_condition = "attribute.repository == \"${var.github_repository_name}\""

  # emissor do token
  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.actor"      = "assertion.actor"
    "attribute.repository" = "assertion.repository"
    "attribute.ref"        = "assertion.ref"
  }
}
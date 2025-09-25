resource "google_service_account" "sa_github_actions" {
  account_id   = "sa-cicd-github"
  display_name = "Service Account para CI/CD via Github Actions"
}

resource "google_service_account" "sa_executor" {
  account_id   = "sa-executor-app"
  display_name = "Service Account para Execução da Aplicação (Runtime)"
  project      = var.project
}
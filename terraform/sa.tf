resource "google_service_account" "sa_github_actions" {
  account_id   = "sa-cicd-github"
  display_name = "Service Account para CI/CD via Github Actions"
}
output "github_actions_sa_email" {
  description = "E-mail da Service Account usada pelo Github Actions"
  value       = google_service_account.sa_github_actions.email
}
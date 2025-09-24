output "github_actions_sa_email" {
  description = "E-mail da Service Account usada pelo Github Actions"
  value       = google_service_account.sa_github_actions.email
}

output "wif_id_completo" {
  description = "Full Resource Name do Workload Identity Provider, usado pelo GitHub Actions."
  value       = google_iam_workload_identity_pool_provider.github_provider.name
}

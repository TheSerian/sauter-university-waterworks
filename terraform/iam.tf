# permissão para gerenciar buckets
resource "google_project_iam_member" "sa_storage_admin" {
  project = var.project
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.sa_github_actions.email}"
}

# permissão para assumir a identidade
resource "google_project_iam_member" "sa_impersonation" {
  project = var.project
  role    = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:${google_service_account.sa_github_actions.email}"
}

# permissão para gerenciar o BigQuery
resource "google_project_iam_member" "sa_bigquery_admin" {
  project = var.project
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.sa_github_actions.email}"
}

# permissão para gerenciar o Cloud Run (deploy da API)
resource "google_project_iam_member" "sa_run_admin" {
  project = var.project
  role    = "roles/run.admin"
  member  = "serviceAccount:${google_service_account.sa_github_actions.email}"
}

# permissão para gerenciar o Artifact Registry
resource "google_project_iam_member" "sa_artifact_registry_admin" {
  project = var.project
  role    = "roles/artifactregistry.admin"
  member  = "serviceAccount:${google_service_account.sa_github_actions.email}"
}

# criar tokens (impersonar) a sa_executor
resource "google_service_account_iam_member" "github_actions_can_impersonate_executor" {
  service_account_id = google_service_account.sa_executor.name 
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:${google_service_account.sa_github_actions.email}" 
}

resource "google_service_account_iam_member" "wi_sa_binding" {
  service_account_id = google_service_account.sa_github_actions.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principal://iam.googleapis.com/${google_iam_workload_identity_pool.github_pool.name}/subject/repo:${var.github_repository_name}"

  depends_on = [
    google_iam_workload_identity_pool_provider.github_provider,
  ]
}

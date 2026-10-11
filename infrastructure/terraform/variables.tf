variable "kubernetes_host" {
  description = "Kubernetes API server address"
  type        = string
  sensitive   = true
}

variable "kubernetes_ca_certificate" {
  description = "Base64 encoded Kubernetes CA certificate"
  type        = string
  sensitive   = true
}

variable "kubernetes_token" {
  description = "Kubernetes service account token"
  type        = string
  sensitive   = true
}

variable "monitoring_namespace" {
  description = "Namespace for monitoring components"
  type        = string
  default     = "monitoring"
}

variable "grafana_admin_password" {
  description = "Mật khẩu tài khoản admin của Grafana"
  type        = string
  sensitive   = true
}
output "monitoring_namespace" {
  description = "Monitoring namespace"
  value       = kubernetes_namespace.monitoring.metadata[0].name
}

output "monitoring_helm_release" {
  description = "kube-prometheus-stack Helm release"
  value       = helm_release.kube_prometheus_stack.name
}

output "argocd_namespace" {
  description = "Argo CD namespace"
  value       = kubernetes_namespace.argocd.metadata[0].name
}

output "argocd_helm_release" {
  description = "Argo CD Helm release"
  value       = helm_release.argocd.name
}
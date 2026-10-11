resource "kubernetes_namespace" "monitoring" {
  metadata {
    name = var.monitoring_namespace
  }
}

resource "helm_release" "kube_prometheus_stack" {
  name      = "monitoring"
  namespace = kubernetes_namespace.monitoring.metadata[0].name

  repository = "https://prometheus-community.github.io/helm-charts"
  chart      = "kube-prometheus-stack"
  timeout    = 900

  values = [
    file("${path.module}/helm/monitoring-values.yaml")
  ]

  # set_sensitive: Terraform ẩn giá trị này khỏi log plan/apply
  set_sensitive {
    name  = "grafana.adminPassword"
    value = var.grafana_admin_password
  }

  depends_on = [
    kubernetes_namespace.monitoring
  ]
}

resource "kubernetes_namespace" "argocd" {
  metadata {
    name = "argocd"
  }
}

resource "helm_release" "argocd" {
  name       = "argocd"
  namespace  = kubernetes_namespace.argocd.metadata[0].name
  repository = "https://argoproj.github.io/argo-helm"
  chart      = "argo-cd"

  depends_on = [
    kubernetes_namespace.argocd
  ]
}

locals {
  # Đọc Application từ argocd/california_housing_application.yaml để chỉ có 1 nguồn sự thật duy nhất
  argocd_app = yamldecode(file("${path.module}/../../argocd/california_housing_application.yaml"))
}

# Dùng chart argocd-apps thay vì kubernetes_manifest, vì kubernetes_manifest
# cần CRD tồn tại ngay lúc plan, mà CRD chỉ có sau khi Argo CD được cài.
resource "helm_release" "argocd_apps" {
  name       = "argocd-apps"
  namespace  = kubernetes_namespace.argocd.metadata[0].name
  repository = "https://argoproj.github.io/argo-helm"
  chart      = "argocd-apps"

  values = [
    yamlencode({
      applications = {
        (local.argocd_app.metadata.name) = {
          namespace   = local.argocd_app.metadata.namespace
          project     = local.argocd_app.spec.project
          source      = local.argocd_app.spec.source
          destination = local.argocd_app.spec.destination
          syncPolicy  = local.argocd_app.spec.syncPolicy
        }
      }
    })
  ]

  depends_on = [helm_release.argocd]
}
# nix/gitops/env/monitoring/grafana-provisioning.nix
#
# Grafana provisioning ConfigMaps: the Prometheus datasource and the file-based
# dashboard provider (scans /etc/grafana/dashboards recursively). Split out of
# the old monolithic monitoring.nix with no behaviour change.
{ mon, ns, dsUid }:
{
  name = "monitoring/grafana-provisioning.yaml";
  content = ''
    apiVersion: v1
    kind: ConfigMap
    metadata:
      name: grafana-datasources
      namespace: ${ns}
    data:
      datasource.yaml: |
        apiVersion: 1
        datasources:
          - name: Prometheus
            uid: ${dsUid}
            type: prometheus
            access: proxy
            url: http://prometheus:${toString mon.prometheus.port}
            isDefault: true
    ---
    apiVersion: v1
    kind: ConfigMap
    metadata:
      name: grafana-dashboard-provider
      namespace: ${ns}
    data:
      provider.yaml: |
        apiVersion: 1
        providers:
          - name: soak
            orgId: 1
            type: file
            disableDeletion: false
            editable: true
            options:
              path: /etc/grafana/dashboards
  '';
}

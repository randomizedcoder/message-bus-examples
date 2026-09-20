# nix/gitops/env/monitoring/grafana.nix
#
# Grafana Service (NodePort) + Deployment, pinned to cp0, anonymous Admin on the
# secure LAN. `volumeMounts`/`volumes` are the per-community-dashboard ConfigMap
# strings from ./dashboards (pre-dedented to the rendered column — see the note
# in dashboards/community.nix). Split out of the old monolithic monitoring.nix
# with no behaviour change.
{ pkgs, mon, ns, cp0Host, volumeMounts, volumes }:
{
  name = "monitoring/grafana.yaml";
  content = ''
    apiVersion: v1
    kind: Service
    metadata:
      name: grafana
      namespace: ${ns}
      labels:
        app: grafana
    spec:
      type: NodePort
      selector:
        app: grafana
      ports:
      - name: web
        port: ${toString mon.grafana.port}
        targetPort: ${toString mon.grafana.port}
        nodePort: ${toString mon.grafana.nodePort}
    ---
    apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: grafana
      namespace: ${ns}
    spec:
      replicas: 1
      strategy:
        type: Recreate
      selector:
        matchLabels:
          app: grafana
      template:
        metadata:
          labels:
            app: grafana
        spec:
          affinity:
            nodeAffinity:
              requiredDuringSchedulingIgnoredDuringExecution:
                nodeSelectorTerms:
                - matchExpressions:
                  - key: kubernetes.io/hostname
                    operator: In
                    values: [ ${cp0Host} ]
          containers:
          - name: grafana
            image: ${mon.grafana.image}:${mon.grafana.tag}
            imagePullPolicy: Never
            # Run from Grafana's real store path, not the image's /share
            # symlink: Grafana 13's plugin loader rejects core-plugin files
            # whose symlink-resolved path escapes the logical plugin dir, so
            # a symlinked homepath crash-loops on startup. The store path has
            # the same files as real dirs. (This is what the NixOS module does.)
            command: ["${pkgs.grafana}/bin/grafana", "server", "--homepath=${pkgs.grafana}/share/grafana"]
            env:
            - name: HOME
              value: /var/lib/grafana
            - name: GF_PATHS_DATA
              value: /var/lib/grafana
            - name: GF_PATHS_LOGS
              value: /var/log/grafana
            - name: GF_PATHS_PLUGINS
              value: /var/lib/grafana/plugins
            - name: GF_PATHS_PROVISIONING
              value: /etc/grafana/provisioning
            - name: GF_AUTH_ANONYMOUS_ENABLED
              value: "true"
            - name: GF_AUTH_ANONYMOUS_ORG_ROLE
              value: "Admin"
            - name: GF_AUTH_DISABLE_LOGIN_FORM
              value: "true"
            - name: GF_ANALYTICS_REPORTING_ENABLED
              value: "false"
            - name: GF_ANALYTICS_CHECK_FOR_UPDATES
              value: "false"
            ports:
            - containerPort: ${toString mon.grafana.port}
              name: web
            readinessProbe:
              httpGet:
                path: /api/health
                port: ${toString mon.grafana.port}
              initialDelaySeconds: 10
              periodSeconds: 15
            livenessProbe:
              httpGet:
                path: /api/health
                port: ${toString mon.grafana.port}
              initialDelaySeconds: 30
              periodSeconds: 30
            volumeMounts:
            - name: datasources
              mountPath: /etc/grafana/provisioning/datasources
            - name: dashboard-provider
              mountPath: /etc/grafana/provisioning/dashboards
            # Sibling subdirs under the provider path (which is scanned
            # recursively); nesting a mount inside the read-only soak
            # ConfigMap mount would not work.
            - name: dashboards
              mountPath: /etc/grafana/dashboards/soak
            # One mount per community dashboard (each its own ConfigMap).
            ${volumeMounts}
            - name: data
              mountPath: /var/lib/grafana
            - name: logs
              mountPath: /var/log/grafana
            resources:
              requests:
                cpu: 50m
                memory: 128Mi
              limits:
                cpu: 500m
                memory: 512Mi
          volumes:
          - name: datasources
            configMap:
              name: grafana-datasources
          - name: dashboard-provider
            configMap:
              name: grafana-dashboard-provider
          - name: dashboards
            configMap:
              name: grafana-dashboards
          ${volumes}
          - name: data
            emptyDir: {}
          - name: logs
            emptyDir: {}
  '';
}

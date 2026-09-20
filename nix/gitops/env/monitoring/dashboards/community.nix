# nix/gitops/env/monitoring/dashboards/community.nix
#
# Community Grafana dashboards (grafana.com), fetched + hash-verified at build
# time, datasource-rewired to our provisioned uid, and emitted as one ConfigMap
# per dashboard (kept well under the ~1 MB etcd object limit — a single combined
# ConfigMap reached ~926 KB with 6 dashboards). Returns the manifest entry plus
# the Grafana volumeMounts/volumes strings for those per-dashboard ConfigMaps.
# Split out of the old monolithic monitoring.nix with no behaviour change.
{ pkgs, lib, ns, dsUid }:
let
  # Each is fetched at build time with fetchurl, pinned to a specific revision
  # AND sha256 (a fixed-output derivation — the content hash is verified, so a
  # changed upstream download fails the build). Only dashboards whose exporters
  # we actually run are included (MQTT/EMQX have no exporter here — omitted).
  communityDashboardDefs = [
    { file = "nats-servers";   id = 2279;  rev = 1;  sha256 = "sha256-CDYAwz1f94fE8jle/y05FgqXVUMAefqCSNvG5gr8cNg="; }
    { file = "nats-jetstream"; id = 14725; rev = 2;  sha256 = "sha256-NPYysyaXAypA+rManlUEwmeB/2zrxGi+tDQQZ19K5rs="; }
    { file = "rabbitmq";       id = 10991; rev = 15; sha256 = "sha256-+Yoh/lDIXB2kHRqaQp0EqlsTfJAldTlF8bU+j2/B5qI="; }
    { file = "valkey";         id = 24733; rev = 2;  sha256 = "sha256-revsZl1eDlO0RKt6xjr/ZfRcTv5hKngxigB6W6+pVRw="; }
    { file = "redis-exporter"; id = 14091; rev = 1;  sha256 = "sha256-OkMixhIT6fkptYrM54HEbt/8BjOqZIltzyWE+TR1RUc="; }
    # Node Exporter Full — host CPU/mem/net/disk/systemd/processes for all 4 VMs.
    # Unlike the others this one has no DS_PROMETHEUS __input; it references a
    # `ds_prometheus` datasource *template variable* instead (handled below).
    { file = "node-exporter-full"; id = 1860; rev = 45; sha256 = "sha256-GExrdAnzBtp1Ul13cvcZRbEM6iOtFrXXjEaY6g6lGYY="; }
  ];
  fetchDash = d: pkgs.fetchurl {
    url = "https://grafana.com/api/dashboards/${toString d.id}/revisions/${toString d.rev}/download";
    inherit (d) sha256;
  };
  # ConfigMap name / volume name / mount subdir per dashboard.
  dashCmName  = d: "grafana-dashboard-${d.file}";
  dashVolName = d: "dash-${d.file}";

  # One ConfigMap *per dashboard*, all emitted into a single multi-document
  # YAML file (--- separated). Splitting per-dashboard keeps every object well
  # under the ~1 MB etcd object limit — a single combined ConfigMap reached
  # ~926 KB with 6 dashboards and would break outright as more are added.
  communityDashboardsCM = pkgs.runCommand "grafana-community-dashboards.yaml"
    { nativeBuildInputs = [ pkgs.jq pkgs.kubectl ]; }
    ''
      : > "$out"
      ${lib.concatMapStringsSep "\n" (d: ''
        # Point every datasource placeholder at our provisioned uid. Community
        # dashboards name this differently — an import __input (DS_PROMETHEUS,
        # DS_NATS-PROMETHEUS, DS__NATS-PROMETHEUS, ...) and/or a datasource-type
        # template variable (ds_prometheus, 1860). Hardcoding one name silently
        # leaves the others dangling -> panels bind to a missing datasource and
        # render red "No data". So derive the exact placeholder names from THIS
        # dashboard's __inputs + datasource-type template vars and rewrite each
        # ${"$"}{name} token; then strip the import-only keys and the now-orphan
        # datasource template var (no dangling picker).
        mkdir -p "${d.file}"
        names=$(jq -r '((.__inputs // [])[] | select(.type=="datasource") | .name),
                       ((.templating.list // [])[] | select(.type=="datasource") | .name)' ${fetchDash d})
        sedargs=()
        for n in $names; do sedargs+=(-e "s|[$]{$n}|${dsUid}|g"); done
        { if [ ''${#sedargs[@]} -gt 0 ]; then sed "''${sedargs[@]}" ${fetchDash d}; else cat ${fetchDash d}; fi; } \
          | jq 'del(.__inputs, .__requires, .__elements)
                | .id = null | .uid = "${d.file}"
                | if (.templating.list | type) == "array"
                  then .templating.list |= map(select(.type != "datasource"))
                  else . end' \
          > "${d.file}/${d.file}.json"
        # ServerSideApply on every dashboard CM: some (e.g. node-exporter-full)
        # exceed the 256 KB cap on the client-side-apply annotation on their
        # own, and the flag is harmless for the small ones.
        echo '---' >> "$out"
        kubectl create configmap ${dashCmName d} \
          --namespace=${ns} --from-file="${d.file}" --dry-run=client -o yaml \
          | kubectl annotate --local -f - -o yaml \
              argocd.argoproj.io/sync-options=ServerSideApply=true >> "$out"
      '') communityDashboardDefs}
    '';
in
{
  # The ConfigMap manifest entry (a Nix-store file, copied through to rendered/).
  manifest = {
    name = "monitoring/grafana-dashboards-community.yaml";
    source = communityDashboardsCM;
  };

  # Grafana volumeMounts / volumes for the per-dashboard ConfigMaps. Each CM is
  # mounted in its own subdir under the provider path (scanned recursively), so
  # the file provider discovers them all. These strings are interpolated into
  # the Deployment's '' block, whose literal lines are dedented by 8 spaces at
  # eval time while interpolated text is not — so the continuation indents here
  # are pre-dedented to the *rendered* column (mounts: 8/10, volumes: 6/8/10).
  # First element gets its base indent from the template line.
  volumeMounts = lib.concatMapStringsSep "\n        " (d:
    "- name: ${dashVolName d}\n          mountPath: /etc/grafana/dashboards/community/${d.file}"
  ) communityDashboardDefs;
  volumes = lib.concatMapStringsSep "\n      " (d:
    "- name: ${dashVolName d}\n        configMap:\n          name: ${dashCmName d}"
  ) communityDashboardDefs;
}

# nix/gitops/helm-chart.nix
#
# Generic helm-template helper for the rendered-manifests pattern.
#
# Usage:
#   renderChart {
#     name        = "cilium";
#     releaseName = "cilium";
#     namespace   = "kube-system";
#     chart       = constants.helmCharts.cilium;  # { url, hash, version }
#     values      = "kubeProxyReplacement: true\n...";  # multi-line YAML string
#   }
# Returns a derivation whose $out/install.yaml is the fully rendered,
# CRDs-included multi-document YAML.
#
{ pkgs, lib }:
rec {
  renderChart =
    { name
    , releaseName
    , namespace
    , chart            # { url; hash; version; } — typically from constants.helmCharts.*
    , values           # YAML string
    , extraArgs ? [ ]  # additional `helm template` flags
    }:
    let
      tarball = pkgs.fetchurl {
        url  = chart.url;
        hash = chart.hash;
      };
      valuesFile = pkgs.writeText "${name}-values.yaml" values;
    in
    pkgs.runCommand "${name}-rendered"
      {
        nativeBuildInputs = [ pkgs.kubernetes-helm pkgs.gnutar pkgs.gzip ];
        passthru = { inherit tarball valuesFile; };
      } ''
      export HOME=$TMPDIR
      mkdir -p $out chart

      # Some helm repos (e.g. helm.cilium.io) serve tarballs with
      # Content-Encoding: gzip, which Nix's fetchurl (via curl) transparently
      # decompresses — leaving us with a plain tar. Detect and normalise by
      # extracting the chart directory either way, then hand the directory
      # to `helm template`.
      if gzip -t ${tarball} 2>/dev/null; then
        tar -xzf ${tarball} -C chart
      else
        tar -xf  ${tarball} -C chart
      fi

      # The archive contains a single top-level dir matching the chart name.
      CHART_DIR="chart/$(ls chart | head -n1)"

      helm template ${releaseName} "$CHART_DIR" \
        --namespace ${namespace} \
        --include-crds \
        --values ${valuesFile} \
        ${lib.concatStringsSep " " extraArgs} \
        > $out/install.yaml
    '';
}

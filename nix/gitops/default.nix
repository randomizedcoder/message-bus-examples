# nix/gitops/default.nix
#
# GitOps manifest generator.
# Generates Kubernetes YAML manifests from Nix using nixidy-style patterns.
# Output: nix build .#k8s-manifests -> result/ directory of YAML files
#
# Each manifest entry is one of two shapes:
#   { name = "ns/file.yaml"; content = "..."; }        # inline string
#   { name = "ns/file.yaml"; source  = <path|drv>; }   # file copied from Nix store
#
# The `source` form lets helm-template outputs flow through to rendered/
# without going via a Nix string (which would IFD-load the contents).
#
{ pkgs, lib, nixidy ? null }:
let
  envDir = ./env;
  helm = import ./helm-chart.nix { inherit pkgs lib; };

  # Import all environment modules
  base     = import (envDir + "/base.nix")     { inherit pkgs lib; };
  argocd   = import (envDir + "/argocd.nix")   { inherit pkgs lib helm; };
  cilium   = import (envDir + "/cilium.nix")   { inherit pkgs lib helm; };
  storage  = import (envDir + "/storage.nix")  { inherit pkgs lib; };

  # Message buses (hand-written clustered StatefulSets, Nix-built images)
  nats     = import (envDir + "/nats.nix")     { inherit pkgs lib; };
  rabbitmq = import (envDir + "/rabbitmq.nix") { inherit pkgs lib; };
  mqtt     = import (envDir + "/mqtt.nix")     { inherit pkgs lib; };
  valkey   = import (envDir + "/valkey.nix")   { inherit pkgs lib; };

  # Observability (in-cluster Prometheus + Grafana + NATS exporter)
  monitoring = import (envDir + "/monitoring.nix") { inherit pkgs lib; };

  # Combine all manifests
  allManifests = base.manifests ++ argocd.manifests ++ cilium.manifests
    ++ storage.manifests
    ++ nats.manifests
    ++ rabbitmq.manifests
    ++ mqtt.manifests
    ++ valkey.manifests
    ++ monitoring.manifests;

  emitStep = m:
    if m ? source then ''
      mkdir -p "$out/$(dirname "${m.name}")"
      cp "${m.source}" "$out/${m.name}"
      chmod u+w "$out/${m.name}"
    '' else ''
      mkdir -p "$out/$(dirname "${m.name}")"
      cat > "$out/${m.name}" <<'MANIFEST_EOF'
      ${m.content}
      MANIFEST_EOF
    '';

  manifestDerivation = pkgs.runCommand "k8s-manifests" { } ''
    mkdir -p $out
    ${lib.concatMapStringsSep "\n" emitStep allManifests}
    echo "Generated ${toString (builtins.length allManifests)} manifests" > $out/README
  '';
in
{
  packages = {
    k8s-manifests = manifestDerivation;
  };
}

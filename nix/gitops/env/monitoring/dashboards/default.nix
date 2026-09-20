# nix/gitops/env/monitoring/dashboards/default.nix
#
# Grafana dashboard manifests: the hand-written soak dashboard and the fetched
# community dashboards. Returns the manifest entries plus the Grafana
# volumeMounts/volumes strings the community per-dashboard ConfigMaps need.
# A proto-bench dashboard (its own ConfigMap, dashboard uid `protobench`) slots
# in here next (P4c).
{ pkgs, lib, ns, dsUid }:
let
  soak      = import ./soak.nix { inherit ns; };
  community = import ./community.nix { inherit pkgs lib ns dsUid; };
in
{
  manifests = [ soak community.manifest ];
  inherit (community) volumeMounts volumes;
}

# nix/gitops/env/monitoring/dashboards/default.nix
#
# Grafana dashboard manifests: the hand-written soak + proto-bench dashboards
# and the fetched community dashboards. Returns the manifest entries plus the
# Grafana volumeMounts/volumes strings the per-dashboard ConfigMaps need.
#
# proto-bench is its own ConfigMap (uid `protobench`), mounted in its own subdir
# under the provider path. Its volumeMount/volume are appended to the community
# strings at the SAME rendered columns (see the indent note in ./community.nix):
# mounts continue at 8 spaces (mountPath 10), volumes at 6 (configMap 8, name 10).
{ pkgs, lib, ns, dsUid }:
let
  soak       = import ./soak.nix { inherit ns; };
  community  = import ./community.nix { inherit pkgs lib ns dsUid; };
  protobench = import ./protobench.nix { inherit ns; };
  rpc        = import ./rpc.nix { inherit ns; };
  grpcbus    = import ./grpcbus.nix { inherit ns; };
  pbMount  = "- name: ${protobench.volName}\n          mountPath: ${protobench.mountPath}";
  pbVolume = "- name: ${protobench.volName}\n        configMap:\n          name: ${protobench.cmName}";
  rpcMount  = "- name: ${rpc.volName}\n          mountPath: ${rpc.mountPath}";
  rpcVolume = "- name: ${rpc.volName}\n        configMap:\n          name: ${rpc.cmName}";
  gbMount  = "- name: ${grpcbus.volName}\n          mountPath: ${grpcbus.mountPath}";
  gbVolume = "- name: ${grpcbus.volName}\n        configMap:\n          name: ${grpcbus.cmName}";
in
{
  manifests = [ soak protobench.manifest rpc.manifest grpcbus.manifest community.manifest ];
  volumeMounts = community.volumeMounts + "\n        " + pbMount + "\n        " + rpcMount + "\n        " + gbMount;
  volumes = community.volumes + "\n      " + pbVolume + "\n      " + rpcVolume + "\n      " + gbVolume;
}

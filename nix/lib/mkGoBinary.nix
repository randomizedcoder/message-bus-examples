# nix/lib/mkGoBinary.nix
#
# Build ONE static Go binary from the shared `clients` module, for images and
# the like — the counterpart to nix/clients.nix (which builds every CLI in one
# derivation for the host `nix run` apps). Same source and vendored deps
# (versions.goVendorHash — `go mod vendor` covers the whole module, so the hash
# is independent of which subPackage we install), but:
#
#   • CGO off + PIE disabled  → a truly static binary that runs in the minimal
#     busybox image with no glibc/loader in the closure.
#   • -pgo=off                → no hidden profile-guided optimisation, so a
#     benchmark binary is comparable run to run (design rule 11).
#   • -trimpath (buildGoModule default) + -ldflags "-s -w -X main.version=…"
#     → reproducible paths, stripped, and a stamped version string.
#
# Usage: import ./mkGoBinary.nix { inherit pkgs versions; } { pname; subPackage; }
{ pkgs, versions }:
{ pname, subPackage, version ? "0.1.0", ldflagsExtra ? [ ] }:
pkgs.buildGoModule {
  inherit pname version;
  src = ../../clients;
  vendorHash = versions.goVendorHash;
  subPackages = [ subPackage ];

  env.CGO_ENABLED = 0;
  # -pgo=off keeps the build free of any default.pgo profile; -trimpath is
  # already added by buildGoModule.
  env.GOFLAGS = "-pgo=off";
  ldflags = [ "-s" "-w" "-X" "main.version=${version}" ] ++ ldflagsExtra;

  # Binaries only; no tests here (nix/clients.nix's `tests` derivation runs
  # `go test ./...` for the whole module and is what `nix flake check` uses).
  doCheck = false;

  meta = {
    description = "Static ${pname} binary (proto-bench, design §9.1)";
    mainProgram = pname;
  };
}

# nix/checks/go-vet.nix
#
# `go vet ./...` over the whole client module, built offline against the same
# vendored deps `buildGoModule` uses (via the goModules passthru + -mod=vendor,
# the pattern proven by clients-bench in nix/clients.nix).
{ pkgs, versions, clients }:
pkgs.runCommand "go-vet"
{
  nativeBuildInputs = [ versions.go ];
} ''
  cp -a ${../../clients}/. src && chmod -R u+w src
  cd src
  mkdir -p vendor
  cp -a ${clients.package.goModules}/. vendor/ && chmod -R u+w vendor
  export HOME=$TMPDIR GOFLAGS=-mod=vendor GOCACHE=$TMPDIR/gocache GOTOOLCHAIN=local CGO_ENABLED=0
  go vet ./...
  touch $out
''

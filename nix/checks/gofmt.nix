# nix/checks/gofmt.nix
#
# gofmt gate over clients/, excluding the generated tree (gen/ is gofmt-clean
# by construction and re-checked by proto-gen-drift).
{ pkgs, versions }:
pkgs.runCommand "gofmt"
{
  nativeBuildInputs = [ versions.go ];
} ''
  cd ${../../clients}
  unformatted=$(gofmt -l $(find . -name '*.go' -not -path './gen/*'))
  if [ -n "$unformatted" ]; then
    echo "gofmt: the following files are not formatted:" >&2
    echo "$unformatted" >&2
    exit 1
  fi
  touch $out
''

# nix/checks/default.nix
#
# Merges every `nix flake check` gate: the existing table-driven client tests,
# the proto gates (lint, breaking, gen-drift), and go-vet + gofmt over the
# client module. Consumed by flake.nix's `checks` output (design §5).
{ pkgs, versions, clients, protos }:
{
  cli-tests = clients.tests;
  go-vet = import ./go-vet.nix { inherit pkgs versions clients; };
  gofmt = import ./gofmt.nix { inherit pkgs versions; };
}
// protos.checks

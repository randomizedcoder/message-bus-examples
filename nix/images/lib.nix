# nix/images/lib.nix
#
# The shared `mkImage` helper for the Nix-built OCI images (buses,
# observability stack, and the proto-bench region-agent). Factored out of
# nix/images/default.nix so nix/images/region-agent.nix can reuse the exact
# same layered-image builder (design §9.1).
{ pkgs }:
{
  # mkImage { name; tag; contents; config ? {} } → docker-archive tarball drv.
  # config merges over a default PATH=/bin (override Env in `config` to add
  # more, keeping /bin in the list).
  mkImage = { name, tag, contents, config ? { } }:
    pkgs.dockerTools.buildLayeredImage {
      inherit name tag contents;
      config = {
        Env = [ "PATH=/bin" ];
      } // config;
    };
}

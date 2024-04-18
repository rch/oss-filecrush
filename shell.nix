let
  sources = import ./nix/sources.nix;
  pkgs = import sources.nixpkgs {};
in
with pkgs;
pkgs.mkShell {
  buildInputs = [
    bash
    git
    gradle
    jdk8
    maven
    postgresql_15
    python311
    python311Packages.psycopg2
  ];
}


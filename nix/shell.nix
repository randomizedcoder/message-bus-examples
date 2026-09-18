# nix/shell.nix
#
# Development shell for K8s cluster.
#
{ pkgs }:
pkgs.mkShell {
  packages = with pkgs; [
    kubectl
    kubernetes-helm
    cilium-cli
    hubble
    argocd
    step-cli
    socat
    expect
    sshpass
    jq
    bc           # float arithmetic for chaos-failover recovery math
    nftables
    iproute2
    curl
    openssl      # secret generation
    # Go client development (nats/rabbitmq/mqtt/valkey CLIs under ./clients)
    go
    gopls
    # Message-bus CLIs for ad-hoc pub/sub from the shell
    natscli
    mosquitto    # mosquitto_pub / mosquitto_sub
    valkey       # valkey-cli
  ];
  shellHook = ''
    echo "message-bus-examples — Development Shell (3 CP + 1 Worker)"
    echo ""
    echo "Quick start:"
    echo "  nix run .#k8s-check-host              # Verify host prereqs"
    echo "  sudo nix run .#k8s-network-setup      # Create network + haproxy LB"
    echo "  nix run .#k8s-gen-secrets             # Generate bus credentials"
    echo "  nix run .#k8s-start-all               # Build + start all VMs"
    echo "  nix run .#k8s-vm-ssh -- --node=cp0    # SSH to cp0"
    echo ""
    echo "Pub/sub from the host (each bus over its NodePort):"
    echo "  nix run .#nats-sub   ;  nix run .#nats-pub -- -msg hello"
    echo "  nix run .#mqtt-sub   ;  nix run .#mqtt-pub -- -msg hello"
    echo "  nix run .#valkey-sub -- -pass \$VALKEY_PASS"
    echo "  nix run .#rabbitmq-sub -- -pass \$RABBITMQ_PASS"
  '';
}

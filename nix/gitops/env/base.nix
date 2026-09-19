# nix/gitops/env/base.nix
#
# Base cluster configuration: namespaces, RBAC.
#
{ pkgs, lib }:
let
  constants = import ../../constants.nix;
in
{
  manifests = [
    {
      name = "base/namespaces.yaml";
      content = ''
        apiVersion: v1
        kind: Namespace
        metadata:
          name: argocd
        ---
        apiVersion: v1
        kind: Namespace
        metadata:
          name: kube-system
        ---
        apiVersion: v1
        kind: Namespace
        metadata:
          name: ${constants.messageBus.nats.namespace}
        ---
        apiVersion: v1
        kind: Namespace
        metadata:
          name: ${constants.messageBus.rabbitmq.namespace}
        ---
        apiVersion: v1
        kind: Namespace
        metadata:
          name: ${constants.messageBus.mqtt.namespace}
        ---
        apiVersion: v1
        kind: Namespace
        metadata:
          name: ${constants.messageBus.valkey.namespace}
        ---
        apiVersion: v1
        kind: Namespace
        metadata:
          name: ${constants.monitoring.namespace}
      '';
    }
    {
      name = "base/coredns.yaml";
      content = ''
        apiVersion: v1
        kind: ServiceAccount
        metadata:
          name: coredns
          namespace: kube-system
        ---
        apiVersion: rbac.authorization.k8s.io/v1
        kind: ClusterRole
        metadata:
          name: system:coredns
        rules:
        - apiGroups: [""]
          resources: ["endpoints", "services", "pods", "namespaces"]
          verbs: ["list", "watch"]
        - apiGroups: ["discovery.k8s.io"]
          resources: ["endpointslices"]
          verbs: ["list", "watch"]
        ---
        apiVersion: rbac.authorization.k8s.io/v1
        kind: ClusterRoleBinding
        metadata:
          name: system:coredns
        roleRef:
          apiGroup: rbac.authorization.k8s.io
          kind: ClusterRole
          name: system:coredns
        subjects:
        - kind: ServiceAccount
          name: coredns
          namespace: kube-system
        ---
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: coredns
          namespace: kube-system
        data:
          Corefile: |
            .:53 {
                errors
                health {
                    lameduck 5s
                }
                ready
                kubernetes ${constants.k8s.clusterDomain} in-addr.arpa ip6.arpa {
                    pods insecure
                    fallthrough in-addr.arpa ip6.arpa
                    ttl 30
                }
                forward . 1.1.1.1 8.8.8.8 {
                    max_concurrent 1000
                }
                cache 30
                reload
                loadbalance
            }
        ---
        apiVersion: apps/v1
        kind: Deployment
        metadata:
          name: coredns
          namespace: kube-system
          labels:
            k8s-app: kube-dns
        spec:
          replicas: 2
          strategy:
            type: RollingUpdate
            rollingUpdate:
              maxUnavailable: 1
          selector:
            matchLabels:
              k8s-app: kube-dns
          template:
            metadata:
              labels:
                k8s-app: kube-dns
            spec:
              serviceAccountName: coredns
              tolerations:
              - key: "node-role.kubernetes.io/control-plane"
                effect: "NoSchedule"
              containers:
              - name: coredns
                image: registry.k8s.io/coredns/coredns:v1.12.0
                args: ["-conf", "/etc/coredns/Corefile"]
                ports:
                - containerPort: 53
                  name: dns
                  protocol: UDP
                - containerPort: 53
                  name: dns-tcp
                  protocol: TCP
                - containerPort: 9153
                  name: metrics
                  protocol: TCP
                livenessProbe:
                  httpGet:
                    path: /health
                    port: 8080
                  initialDelaySeconds: 10
                  timeoutSeconds: 5
                readinessProbe:
                  httpGet:
                    path: /ready
                    port: 8181
                resources:
                  requests:
                    cpu: 50m
                    memory: 64Mi
                volumeMounts:
                - name: config-volume
                  mountPath: /etc/coredns
                  readOnly: true
              volumes:
              - name: config-volume
                configMap:
                  name: coredns
        ---
        apiVersion: v1
        kind: Service
        metadata:
          name: kube-dns
          namespace: kube-system
          labels:
            k8s-app: kube-dns
        spec:
          clusterIP: ${constants.k8s.dnsServiceIp}
          selector:
            k8s-app: kube-dns
          ports:
          - name: dns
            port: 53
            targetPort: 53
            protocol: UDP
          - name: dns-tcp
            port: 53
            targetPort: 53
            protocol: TCP
          - name: metrics
            port: 9153
            targetPort: 9153
            protocol: TCP
      '';
    }
    {
      name = "base/rbac.yaml";
      content = ''
        apiVersion: rbac.authorization.k8s.io/v1
        kind: ClusterRoleBinding
        metadata:
          name: system:kube-apiserver
        roleRef:
          apiGroup: rbac.authorization.k8s.io
          kind: ClusterRole
          name: system:kube-apiserver-to-kubelet
        subjects:
        - apiGroup: rbac.authorization.k8s.io
          kind: User
          name: kube-apiserver
        - apiGroup: rbac.authorization.k8s.io
          kind: User
          name: apiserver-kubelet-client
        ---
        apiVersion: rbac.authorization.k8s.io/v1
        kind: ClusterRole
        metadata:
          name: system:kube-apiserver-to-kubelet
        rules:
        - apiGroups: [""]
          resources: ["nodes/proxy", "nodes/stats", "nodes/log", "nodes/spec", "nodes/metrics"]
          verbs: ["*"]
      '';
    }
    {
      name = "base/application.yaml";
      content = ''
        apiVersion: argoproj.io/v1alpha1
        kind: Application
        metadata:
          name: base
          namespace: argocd
        spec:
          project: default
          source:
            repoURL: ${constants.gitops.repoURL}
            targetRevision: ${constants.gitops.targetRevision}
            path: ${constants.gitops.renderedPath}/base
            directory:
              exclude: 'application.yaml'
          destination:
            server: https://kubernetes.default.svc
          syncPolicy:
            automated:
              prune: true
              selfHeal: true
      '';
    }
  ];
}

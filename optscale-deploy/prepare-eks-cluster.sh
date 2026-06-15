#!/usr/bin/env bash

set -euo pipefail

RESET="\033[0m"
BOLD="\033[1m"
CYAN="\033[1;36m"
GREEN="\033[1;32m"
YELLOW="\033[1;33m"
RED="\033[1;31m"

log_info()  { echo -e "${CYAN}${BOLD}[INFO]${RESET}  ${*}"; }
log_ok()    { echo -e "${GREEN}${BOLD}[OK]${RESET}    ${*}"; }
log_warn()  { echo -e "${YELLOW}${BOLD}[WARN]${RESET}  ${*}"; }
log_error() { echo -e "${RED}${BOLD}[ERROR]${RESET} ${*}"; }

SCRIPT_DIR=$(cd "${BASH_SOURCE[0]%/*}" && pwd)

function setup_helm_repos() {
    helm repo add bitnami https://raw.githubusercontent.com/bitnami/charts/refs/heads/archive-full-index/bitnami/ || log_warn "Bitnami repo already exists"
    helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx || log_warn "ingress-nginx repo already exists"
    log_ok "Helm repositories configured"
}

function generate_ssl_cert() {
    if kubectl get secret defaultcert &>/dev/null; then
        log_info "SSL certificate 'defaultcert' already exists, skipping"
        return 0
    fi

    local temp_dir
    temp_dir=$(mktemp -d)
    trap "rm -rf ${temp_dir}" EXIT

    cat > "${temp_dir}/openssl.cfg" <<EOF
[req]
req_extensions = v3_req
distinguished_name = req_distinguished_name
prompt = no

[req_distinguished_name]
stateOrProvinceName = Amsterdam
countryName      = NL
organizationName = Hystax BV
localityName     = Amsterdam

[ v3_req ]
basicConstraints = CA:FALSE
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = *.elb.amazonaws.com
DNS.3 = *.eks.amazonaws.com
IP.1 = 127.0.0.1
EOF

    openssl req -x509 -sha256 -nodes -newkey rsa:4096 \
        -keyout "${temp_dir}/key.pem" \
        -out "${temp_dir}/cert.pem" \
        -extensions v3_req \
        -days 9001 \
        -config "${temp_dir}/openssl.cfg"

    kubectl create secret tls defaultcert \
        --key "${temp_dir}/key.pem" \
        --cert "${temp_dir}/cert.pem"

    log_ok "SSL certificate created"
}

function install_nginx_ingress() {
    if helm list -A | grep -q "ngingress"; then
        log_info "nginx-ingress already installed, skipping"
        return 0
    fi

    local values_file="${SCRIPT_DIR}/ansible/roles/k8s-configure/files/nginx-ingress/values.yaml"

    if [[ -f ${values_file} ]]; then
        helm upgrade --install ngingress ingress-nginx \
            --repo https://kubernetes.github.io/ingress-nginx \
            -f "${values_file}"
    else
        log_warn "Custom values file not found, using default configuration"
        helm upgrade --install ngingress ingress-nginx \
            --repo https://kubernetes.github.io/ingress-nginx \
            --set controller.service.type=LoadBalancer \
            --set controller.service.annotations."service\.beta\.kubernetes\.io/aws-load-balancer-type"="nlb" \
            --set controller.service.annotations."service\.beta\.kubernetes\.io/aws-load-balancer-cross-zone-load-balancing-enabled"="true" \
            --set controller.extraArgs.default-ssl-certificate=default/defaultcert \
            --set controller.ingressClassResource.default=true
    fi

    log_ok "NGINX Ingress Controller installed"

    kubectl wait --namespace default \
        --for=condition=ready pod \
        --selector=app.kubernetes.io/component=controller \
        --timeout=300s || log_warn "Timeout waiting for nginx-ingress pods"

    local lb_url
    lb_url=$(kubectl get svc ngingress-ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || true)
    [[ -n ${lb_url} ]] && log_ok "LoadBalancer: ${lb_url}"
}

function deploy_daemon_set_to_configure_storage_dir() {
    kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: optscale-storage-init
  namespace: default
spec:
  selector:
    matchLabels:
      name: optscale-storage-init
  template:
    metadata:
      labels:
        name: optscale-storage-init
    spec:
      hostPID: true
      hostNetwork: true
      containers:
      - name: init
        image: busybox:latest
        command:
        - sh
        - -c
        - |
          mkdir -p /host/optscale/mariadb
          mkdir -p /host/optscale/mongo
          mkdir -p /host/optscale/rabbitmq
          mkdir -p /host/optscale/etcd
          mkdir -p /host/optscale/minio
          mkdir -p /host/optscale/influxdb
          mkdir -p /host/optscale/clickhouse
          mkdir -p /host/optscale/thanos_receive
          mkdir -p /host/optscale/thanos_storegateway
          mkdir -p /host/optscale/thanos_compactor
          chmod -R 777 /host/optscale
          echo "Storage directories created successfully"
          sleep infinity
        securityContext:
          privileged: true
        volumeMounts:
        - name: host
          mountPath: /host
      volumes:
      - name: host
        hostPath:
          path: /
          type: Directory
EOF

    kubectl rollout status daemonset/optscale-storage-init --timeout=120s || log_warn "Timeout waiting for storage init"
    log_ok "Storage directories created on all nodes"
}

function configure_cluster() {
    kubectl taint nodes --all node-role.kubernetes.io/control-plane- 2>/dev/null || true
    kubectl taint nodes --all node-role.kubernetes.io/master- 2>/dev/null || true
    log_ok "Cluster settings configured"
}

function show_summary() {
    echo ""
    echo -e "${GREEN}✓${RESET} Helm repositories configured"
    echo -e "${GREEN}✓${RESET} SSL certificate ready"
    echo -e "${GREEN}✓${RESET} NGINX Ingress Controller installed"
    echo -e "${GREEN}✓${RESET} Storage directories created"
    echo -e "${GREEN}✓${RESET} Cluster configured"

    local lb_url
    lb_url=$(kubectl get svc ngingress-ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || true)
    [[ -n ${lb_url} ]] && log_info "LoadBalancer: https://${lb_url}"
    log_ok "Done!"
}

setup_helm_repos
generate_ssl_cert
install_nginx_ingress
deploy_daemon_set_to_configure_storage_dir
configure_cluster
show_summary

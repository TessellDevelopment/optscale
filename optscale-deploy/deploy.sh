#!/usr/bin/env bash
set -euo pipefail

# Constants
readonly CHART_NAME=optscale
readonly JOB_NAME=configurator
readonly K8S_NAMESPACE=default
readonly COMPONENTS_FILE=components.yaml
readonly TEMP_DIR=tmp
readonly BASE_OVERLAY=${TEMP_DIR}/base_overlay
readonly CERT_MANAGER_CHART_VERSION=v1.14.4
readonly TAILSCALE_CHART_VERSION=1.84.0

export KUBECONFIG=${HOME}/.kube/config
VERBOSE=false
ACTION=
OVERLAY=
WITH_ELK=true
WAIT_TIMEOUT=300
REGISTRY=index.docker.io/tesselldev
TAG=
NAME=optscale-prod

function log_info()  { echo "$(date +%H:%M:%S): ${*}"; }
function log_debug() { ${VERBOSE} && echo "$(date +%H:%M:%S): [DEBUG] ${*}" || true; }
function log_error() { echo "$(date +%H:%M:%S): [ERROR] ${*}" >&2; }

function usage() {
  cat <<EOF
Usage: $(basename "${0}") --tag TAG [OPTIONS]

Required:
      --tag TAG                  Image tag to deploy

Options:
  -o, --overlay FILE             Overlay config file
      --dry-run                  Helm dry-run to validate chart
  -w, --wait SECONDS             Wait timeout for deployment (default: 300)
      --registry REGISTRY        Docker registry (default: index.docker.io/tesselldev)
  -v, --verbose                  Enable debug logging
  -h, --help                     Show this help
EOF
  exit 0
}

function parse_args() {
  while [[ ${#} -gt 0 ]]; do
    case ${1} in
      -o|--overlay)          OVERLAY=${2}; shift 2 ;;
      --dry-run)             ACTION=dry-run; shift ;;
      -w|--wait)             WAIT_TIMEOUT=${2}; shift 2 ;;
      --registry)            REGISTRY=${2}; shift 2 ;;
      --tag)                 TAG=${2}; shift 2 ;;
      -v|--verbose)          VERBOSE=true; shift ;;
      -h|--help)             usage ;;
      -*)                    log_error "Unknown option: ${1}"; usage ;;
      *)  log_error "Unknown option: ${1}"; usage ;;
    esac
  done

  if [[ -z ${TAG} ]]; then
    log_error "tag is required"; usage
  fi
}

function generate_base_overlay() {
  local master_ip
  master_ip=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}' | sed -E 's|https?://||;s|:[0-9]+$||')
  mkdir -p ${TEMP_DIR}

  {
    if kubectl get secret defaultcert -n ${K8S_NAMESPACE} &>/dev/null; then
      echo "optscale_key: |"
      kubectl get secret defaultcert -n ${K8S_NAMESPACE} -o jsonpath='{.data.tls\.key}' | base64 -d | sed 's/^/  /'
      echo "certificates:"
      echo "  optscale: |"
      kubectl get secret defaultcert -n ${K8S_NAMESPACE} -o jsonpath='{.data.tls\.crt}' | base64 -d | sed 's/^/    /'
    fi

    echo "public_ip: ${master_ip}"
    echo "docker_registry: ${REGISTRY}"
    echo "docker_tag: ${TAG}"
    echo "release: ${NAME}"

    [[ -n ${OVERLAY} ]] && echo "overlay_list: ${OVERLAY}"

    ${WITH_ELK}              && echo -e "elk:\n  enabled: true"

    echo "nodes:"
    for node in $(kubectl get nodes -o jsonpath='{.items[*].metadata.labels.kubernetes\.io/hostname}'); do
      echo "- ${node}"
    done
  } > ${BASE_OVERLAY}
}

function generate_component_versions() {
  local outfile=${CHART_NAME}/component_versions.yaml
  {
    echo "optscale: ${TAG}"
    echo "images:"
    while IFS=: read -r component _; do
      component=${component// /}
      [[ -z ${component} ]] && continue
      [[ ${component} == elk ]] && ! ${WITH_ELK} && continue
      echo "  ${component}: ${TAG}"
    done < ${COMPONENTS_FILE}
  } > ${outfile}
}

function remove_old_jobs() {
  local jobs
  jobs=$(kubectl get jobs -n ${K8S_NAMESPACE} -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)
  for job in ${jobs}; do
    [[ ${job} == *${JOB_NAME}* ]] && kubectl delete job ${job} -n ${K8S_NAMESPACE} --wait=true 2>/dev/null || true
  done
}

function delete_configured_key() {
  local pod
  pod=$(kubectl get pods -n ${K8S_NAMESPACE} -l app=etcd -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  [[ -n ${pod} ]] && kubectl exec ${pod} -n ${K8S_NAMESPACE} -- etcdctl rm configured 2>/dev/null || true
}

function install_cert_manager() {
  helm repo add jetstack https://charts.jetstack.io 2>/dev/null || true
  helm upgrade --install cert-manager jetstack/cert-manager \
    --namespace cert-manager --create-namespace \
    --version ${CERT_MANAGER_CHART_VERSION} --set installCRDs=true \
    --wait --timeout ${WAIT_TIMEOUT}s
}

function install_tailscale_operator() {
  local client_id=${1} client_secret=${2}
  shift 2
  local default_tags=("${@}")

  kubectl create namespace tailscale 2>/dev/null || true
  helm repo add tailscale https://pkgs.tailscale.com/helmcharts 2>/dev/null || true

  local helm_cmd=(helm upgrade --install tailscale-operator tailscale/tailscale-operator
    --namespace tailscale --create-namespace --version ${TAILSCALE_CHART_VERSION}
    --set-string "oauth.clientId=${client_id}" --set-string "oauth.clientSecret=${client_secret}"
    --wait --timeout ${WAIT_TIMEOUT}s)

  if [[ ${#default_tags[@]} -gt 0 ]]; then
    local tags_json
    tags_json=$(printf ',"%s"' "${default_tags[@]}")
    helm_cmd+=(--set-json "operatorConfig.defaultTags=[${tags_json:1}]")
  fi

  "${helm_cmd[@]}"
}

function setup_pre_deploy() {
  [[ -z ${OVERLAY} || ! -f ${OVERLAY} ]] && return

  local cert_manager_enabled
  cert_manager_enabled=$(yq '.ssl.cert_manager.enabled' ${OVERLAY})

  local ts_client_id ts_client_secret
  ts_client_id=$(yq '.tailscale.oauth.client_id' ${OVERLAY})
  ts_client_secret=$(yq '.tailscale.oauth.client_secret' ${OVERLAY})

  local ts_default_tags=()
  while IFS= read -r tag; do
    [[ -n ${tag} && ${tag} != null ]] && ts_default_tags+=("${tag}")
  done < <(yq '.tailscale.defaultTags[]' ${OVERLAY} 2>/dev/null)

  [[ ${cert_manager_enabled} == true ]] && install_cert_manager
  [[ -n ${ts_client_id} && ${ts_client_id} != null && -n ${ts_client_secret} && ${ts_client_secret} != null ]] && \
    install_tailscale_operator ${ts_client_id} ${ts_client_secret} "${ts_default_tags[@]}"
}

function helm_deploy() {
  local is_dry_run=${1}
  mkdir -p ${TEMP_DIR}

  local overlay_args=()
  generate_base_overlay
  overlay_args+=(-f ${BASE_OVERLAY})
  [[ -n ${OVERLAY} ]] && overlay_args+=(-f ${OVERLAY})

  [[ ${is_dry_run} == false ]] && setup_pre_deploy

  generate_component_versions

  local helm_cmd=(helm upgrade --install "${overlay_args[@]}" ${NAME} ${CHART_NAME}
    --wait --timeout ${WAIT_TIMEOUT}s)

  if [[ ${is_dry_run} == true ]]; then
    helm_cmd+=(--debug --dry-run)
  else
    delete_configured_key
    remove_old_jobs
  fi

  log_info "Deploying ${CHART_NAME} release=${NAME}"
  log_debug "Command: ${helm_cmd[*]}"
  "${helm_cmd[@]}"
}

function main() {
  parse_args "${@}"
  case ${ACTION} in
    dry-run) helm_deploy true ;;
    *)     helm_deploy false ;;
  esac
}

main "${@}"
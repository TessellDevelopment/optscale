#!/usr/bin/env bash
# Usage: ./build.sh [component ...] [--tag tag] [--push] [-r registry] [-u username] [-p password] [--no-cache]

set -e

RESET="\033[0m"
BOLD="\033[1m"
CYAN="\033[1;36m"
GREEN="\033[1;32m"
YELLOW="\033[1;33m"
RED="\033[1;31m"
GREY="\033[0;37m"

log_info()    { echo -e "${CYAN}${BOLD}[INFO]${RESET}  ${*}"; }
log_queue()   { echo -e "${GREY}[QUEUE]${RESET} ${*}"; }
log_ok()      { echo -e "${GREEN}${BOLD}[OK]${RESET}    ${*}"; }
log_warn()    { echo -e "${YELLOW}${BOLD}[WARN]${RESET}  ${*}"; }
log_error()   { echo -e "${RED}${BOLD}[ERROR]${RESET} ${*}"; }

REGISTRY=""
LOGIN=""
PASSWORD=""
COMPONENTS_LIST=()
BUILD_TAG=""
FLAGS=""
PUSH=false

function parse_args() {
    while [[ $# -gt 0 ]]; do
        case ${1} in
            --tag)        BUILD_TAG=${2}; shift ;;
            --push)       PUSH=true ;;
            -r)           REGISTRY=${2}; shift ;;
            -u)           LOGIN=${2}; shift ;;
            -p)           PASSWORD=${2}; shift ;;
            --no-cache)   FLAGS="--no-cache" ;;
            *)            COMPONENTS_LIST+=(${1}) ;;
        esac
        shift
    done
    BUILD_TAG=${BUILD_TAG:-local}
}

function login_registry() {
    [[ ${PUSH} != true ]] && return 0
    docker login -u ${LOGIN} -p ${PASSWORD}
}

function discover_dockerfiles() {
    local cmd="find . -mindepth 2 -maxdepth 3 -print | grep Dockerfile | grep -vE '(test|.j2)'"

    if [[ ${#COMPONENTS_LIST[@]} -gt 0 ]]; then
        local pattern
        pattern=$(printf "|%s" "${COMPONENTS_LIST[@]}")
        pattern=${pattern:1}
        cmd="${cmd} | grep -E '(${pattern})/'"
    fi

    eval "${cmd}"
}

function build_and_push_component() {
    local dockerfile=${1}
    local component=${2}

    log_info "[${BOLD}${component}${RESET}] Starting build..."
    docker build ${FLAGS} -t "${component}:${BUILD_TAG}" -f "${dockerfile}" . --platform linux/amd64 \
        || { log_error "[${BOLD}${component}${RESET}] Build failed"; return 1; }
    log_ok "[${BOLD}${component}${RESET}] Build successful"

    [[ ${PUSH} != true ]] && return 0

    log_info "[${BOLD}${component}${RESET}] Starting push..."
    docker tag ${component}:${BUILD_TAG} ${REGISTRY}/${component}:${BUILD_TAG} \
        && docker push ${REGISTRY}/${component}:${BUILD_TAG} \
        || { log_error "[${BOLD}${component}${RESET}] Push failed"; return 1; }
    log_ok "[${BOLD}${component}${RESET}] Push successful"
}

function run_builds() {
    local -a pids
    local -a components

    while IFS= read -r dockerfile; do
        local component=${dockerfile%/*}
        component=${component##*/}
        log_queue "Queuing ${BOLD}${component}${RESET} — tag: ${YELLOW}${BUILD_TAG}${RESET}"
        build_and_push_component "${dockerfile}" "${component}" &
        pids+=($!)
        components+=("${component}")
    done < <(discover_dockerfiles)

    log_info "=== All builds started, waiting for completion ==="
    log_info "Total components: ${BOLD}${#pids[@]}${RESET}"

    local failed=false
    local -a failed_components

    for i in "${!pids[@]}"; do
        log_info "Waiting for ${BOLD}${components[${i}]}${RESET} (PID: ${pids[${i}]})..."
        if wait "${pids[${i}]}"; then
            log_ok "${BOLD}${components[${i}]}${RESET} completed successfully"
        else
            log_error "${BOLD}${components[${i}]}${RESET} failed with exit code $?"
            failed=true
            failed_components+=("${components[${i}]}")
        fi
    done

    print_summary "${#pids[@]}" "${failed}" "${failed_components[@]}"
}

function print_summary() {
    local total=${1}
    local failed=${2}
    shift 2
    local failed_components=("$@")

    echo ""
    log_info "=== Build Summary ==="
    log_info "Total components: ${BOLD}${total}${RESET}"

    if [[ ${failed} == true ]]; then
        log_warn "Failed components: ${#failed_components[@]}"
        log_warn "Failed: ${failed_components[*]}"
        echo ""
        log_error "❌ Build failed!"
        exit 1
    fi

    log_ok "✓ All components built successfully!"
}

parse_args "$@"
login_registry
run_builds

#!/usr/bin/env bash
# Usage: ./retrigger_imports.sh [ACCOUNT_ID ...] [--all] [--cloud-type aws|azure|gcp] [--from-date YYYY-MM-DD]

set -e

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

USER_CONFIG="optscale-deploy/overlay/user_config.yml"
MYSQL_HOST="mariadb"
MYSQL_PORT="3306"
MYSQL_USER="root"
MYSQL_PASSWORD="my-password-01"
MYSQL_DB="my-db"
MYSQL_BIN="/opt/homebrew/opt/mysql-client/bin/mysql"
REST_API_URL="http://restapi:80"

# Inclusion list — if not empty, ONLY these datasources are processed (exclusion list is ignored)
# Applied only in --all mode
# INCLUSION_LIST=(
#     "citizens_prod_infra"
#     "dbai-rnd"
#     "devtest-infra"
#     "forbes_byoa_1"
#     "JD"
#     "native-cp-rnd"
#     "ops-production"
#     "staging-infra"
#     "starship-dev"
#     "starship-prod"
#     "tessell-ops"
#     "tessell-poc-dp"
# )
INCLUSION_LIST=()

# Exclusion list — ignored when INCLUSION_LIST is not empty
# Applied only in --all mode
# EXCLUSION_LIST=(
#     "citizens_dbservices_0"
#     "Tessell"
#     "canary-byoa-0"
#     "qa-dataplane-byoa-3"
#     "finops"
#     "tessell_dba_0"
# )
EXCLUSION_LIST=()

ALL_ACCOUNTS=false
CLOUD_TYPE=""
FROM_DATE=""
ACCOUNT_IDS_INPUT=()
ACCOUNT_IDS=()
ACCOUNT_NAMES=()
CLUSTER_SECRET=""
FROM_DATE_TIMESTAMP=""

function parse_args() {
    while [[ $# -gt 0 ]]; do
        case ${1} in
            --all)          ALL_ACCOUNTS=true ;;
            --cloud-type)   CLOUD_TYPE=${2}; shift ;;
            --from-date)    FROM_DATE=${2}; shift ;;
            *)              ACCOUNT_IDS_INPUT+=(${1}) ;;
        esac
        shift
    done

    if [[ ${ALL_ACCOUNTS} != true && ${#ACCOUNT_IDS_INPUT[@]} -eq 0 ]]; then
        log_error "Provide at least one account ID or use --all"
        exit 1
    fi

    if [[ -z ${FROM_DATE} ]]; then
        log_error "--from-date is required"
        exit 1
    fi

    if [[ -n ${CLOUD_TYPE} ]]; then
        case ${CLOUD_TYPE} in
            aws)   CLOUD_TYPE="aws_cnr" ;;
            azure) CLOUD_TYPE="azure_cnr" ;;
            gcp)   CLOUD_TYPE="gcp_cnr" ;;
            *) log_error "Invalid cloud type '${CLOUD_TYPE}' — use: aws, azure, gcp"; exit 1 ;;
        esac
    fi
}

function is_name_in_list() {
    local target=${1}; shift
    for item in "${@}"; do
        [[ ${item} == ${target} ]] && return 0
    done
    return 1
}

function display_datasource_filter_config() {
    [[ ${ALL_ACCOUNTS} != true ]] && return 0
    [[ ${#INCLUSION_LIST[@]} -gt 0 ]] && { log_info "Inclusion filter: ${#INCLUSION_LIST[@]} datasource(s)"; return 0; }
    [[ ${#EXCLUSION_LIST[@]} -gt 0 ]] && { log_warn "Exclusion filter: ${#EXCLUSION_LIST[@]} datasource(s)"; return 0; }
    log_info "No filter — all${CLOUD_TYPE:+ ${CLOUD_TYPE}} accounts will be processed"
}

function read_cluster_secret_from_config() {
    [[ ! -f ${USER_CONFIG} ]] && { log_error "${USER_CONFIG} not found — run from the OptScale repository root"; exit 1; }
    CLUSTER_SECRET=$(yq '.secrets.cluster' "${USER_CONFIG}")
    [[ -z ${CLUSTER_SECRET} || ${CLUSTER_SECRET} == "null" ]] \
        && { log_error "Could not find cluster secret in ${USER_CONFIG}"; exit 1; }
    log_ok "Cluster secret found"
}

function wait_for_kubefwd_ready() {
    log_warn "Requires kubefwd — run: ${BOLD}sudo kubefwd svc${RESET}"
    read -p "Press Enter once kubefwd is running..."
}

function validate_and_resolve_from_date() {
    log_info "Date-based import: ${BOLD}${FROM_DATE}${RESET}"
    if ! [[ ${FROM_DATE} =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
        log_error "Invalid date format — use YYYY-MM-DD (e.g. 2026-04-01)"
        exit 1
    fi

    FROM_DATE_TIMESTAMP=$("${MYSQL_BIN}" -h${MYSQL_HOST} -P${MYSQL_PORT} -u${MYSQL_USER} -p${MYSQL_PASSWORD} -D${MYSQL_DB} -N -s -e \
        "SELECT UNIX_TIMESTAMP('${FROM_DATE} 00:00:00');")
    [[ -z ${FROM_DATE_TIMESTAMP} || ${FROM_DATE_TIMESTAMP} == "NULL" ]] \
        && { log_error "Failed to convert date to timestamp"; exit 1; }
    log_ok "Date validated: ${FROM_DATE} → timestamp ${FROM_DATE_TIMESTAMP}"
    echo ""
}

function verify_mariadb_connection() {
    "${MYSQL_BIN}" -h${MYSQL_HOST} -P${MYSQL_PORT} -u${MYSQL_USER} -p${MYSQL_PASSWORD} ${MYSQL_DB} \
        -e "SELECT 1;" &>/dev/null \
        || { log_error "Could not connect to MariaDB — is kubefwd running?"; exit 1; }
    log_ok "Connected"
    echo ""
}

function fetch_and_filter_accounts() {
    if [[ ${ALL_ACCOUNTS} == true ]]; then
        local where="deleted_at = 0"
        [[ -n ${CLOUD_TYPE} ]] && where="${where} AND type = '${CLOUD_TYPE}'"

        log_info "Fetching all active${CLOUD_TYPE:+ ${CLOUD_TYPE}} accounts..."
        local raw
        raw=$("${MYSQL_BIN}" -h${MYSQL_HOST} -P${MYSQL_PORT} -u${MYSQL_USER} -p${MYSQL_PASSWORD} ${MYSQL_DB} -N -e \
            "SELECT id, name FROM cloudaccount WHERE ${where} ORDER BY name;")
        [[ -z ${raw} ]] && { log_error "No active${CLOUD_TYPE:+ ${CLOUD_TYPE}} accounts found"; exit 1; }
        log_ok "Found $(echo "${raw}" | wc -l | tr -d ' ') total account(s)"

        local -a excluded_names

        while IFS=$'\t' read -r id name; do
            if [[ ${#INCLUSION_LIST[@]} -gt 0 ]]; then
                is_name_in_list "${name}" "${INCLUSION_LIST[@]}" \
                    && { ACCOUNT_IDS+=("${id}"); ACCOUNT_NAMES+=("${name}"); }
            else
                if is_name_in_list "${name}" "${EXCLUSION_LIST[@]}"; then
                    excluded_names+=("${name}")
                else
                    ACCOUNT_IDS+=("${id}"); ACCOUNT_NAMES+=("${name}")
                fi
            fi
        done <<< "${raw}"

        [[ ${#INCLUSION_LIST[@]} -gt 0 ]] \
            && log_ok "  Matched: ${#ACCOUNT_IDS[@]} / ${#INCLUSION_LIST[@]}" \
            || { [[ ${#excluded_names[@]} -gt 0 ]] && log_warn "  Excluded: ${#excluded_names[@]}"; log_ok "  To process: ${#ACCOUNT_IDS[@]}"; }
    else
        for id in "${ACCOUNT_IDS_INPUT[@]}"; do
            local name
            name=$("${MYSQL_BIN}" -h${MYSQL_HOST} -P${MYSQL_PORT} -u${MYSQL_USER} -p${MYSQL_PASSWORD} ${MYSQL_DB} -N -s -e \
                "SELECT name FROM cloudaccount WHERE id='${id}' AND deleted_at=0;")
            if [[ -z ${name} ]]; then
                log_warn "Account not found or deleted: ${id}"
            else
                ACCOUNT_IDS+=("${id}"); ACCOUNT_NAMES+=("${name}")
            fi
        done
        [[ ${#ACCOUNT_IDS[@]} -eq 0 ]] && { log_error "No valid accounts found"; exit 1; }
    fi

    [[ ${#ACCOUNT_IDS[@]} -eq 0 ]] && { log_warn "No accounts to process"; exit 0; }

    echo ""
    log_info "Accounts to process:"
    for i in "${!ACCOUNT_IDS[@]}"; do
        echo "  $((i+1)). ${ACCOUNT_NAMES[${i}]} (${ACCOUNT_IDS[${i}]})"
    done
    echo ""
}

function confirm_import_action() {
    log_warn "Will retrigger imports for ${#ACCOUNT_IDS[@]} account(s) — reset to ${FROM_DATE}"
    read -p "Continue? (yes/no): " CONFIRM
    [[ ${CONFIRM} != "yes" ]] && { log_warn "Aborted"; exit 0; }
    echo ""
}

function reset_import_timestamps_to_date() {
    log_info "Resetting timestamps to ${FROM_DATE}..."
    for i in "${!ACCOUNT_IDS[@]}"; do
        local ca_id=${ACCOUNT_IDS[${i}]}
        echo -n "${ACCOUNT_NAMES[${i}]}... "
        if "${MYSQL_BIN}" -h${MYSQL_HOST} -P${MYSQL_PORT} -u${MYSQL_USER} -p${MYSQL_PASSWORD} -D${MYSQL_DB} -e "
            UPDATE cloudaccount
            SET last_import_at = ${FROM_DATE_TIMESTAMP},
                last_import_modified_at = ${FROM_DATE_TIMESTAMP},
                last_import_attempt_at = 0,
                last_import_attempt_error = NULL
            WHERE id = '${ca_id}' AND deleted_at = 0;" 2>/dev/null; then
            echo -e "${GREEN}✓${RESET}"
        else
            echo -e "${RED}✗${RESET}"
        fi
    done
    echo ""
    sleep 1
}

function trigger_scheduled_imports() {
    log_info "Triggering imports..."
    echo ""

    local success_count=0
    local fail_count=0
    local -a failed_accounts

    for i in "${!ACCOUNT_IDS[@]}"; do
        local ca_id=${ACCOUNT_IDS[${i}]}
        echo -n "${ACCOUNT_NAMES[${i}]}... "
        local http_code
        http_code=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
            -H "Content-Type: application/json" \
            -H "Secret: ${CLUSTER_SECRET}" \
            -d "{\"cloud_account_id\": \"${ca_id}\"}" \
            "${REST_API_URL}/restapi/v2/schedule_imports")

        if [[ ${http_code} == "201" ]]; then
            echo -e "${GREEN}✓${RESET}"
            success_count=$((success_count + 1))
        else
            echo -e "${RED}✗ (HTTP ${http_code})${RESET}"
            fail_count=$((fail_count + 1))
            failed_accounts+=("${ca_id}")
        fi
        sleep 0.3
    done

    print_import_summary "${success_count}" "${fail_count}" "${failed_accounts[@]}"
}

function print_import_summary() {
    local success_count=${1}
    local fail_count=${2}
    shift 2
    local -a failed_accounts=("$@")

    echo ""
    [[ -n ${CLOUD_TYPE} ]] && log_info "Cloud type: ${CLOUD_TYPE}"
    log_info "Reset date: ${FROM_DATE}"
    log_ok "Triggered: ${success_count}"

    if [[ ${fail_count} -gt 0 ]]; then
        log_error "Failed: ${fail_count}"
        for id in "${failed_accounts[@]}"; do echo "  ${id}"; done
    fi
    log_ok "Done!"
}

parse_args "$@"
display_datasource_filter_config
read_cluster_secret_from_config
wait_for_kubefwd_ready
validate_and_resolve_from_date
verify_mariadb_connection
fetch_and_filter_accounts
confirm_import_action
reset_import_timestamps_to_date
trigger_scheduled_imports

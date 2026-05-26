#!/bin/bash

##############################################################################
# Description: Retrigger imports for all AWS CNR datasources with inclusion/exclusion list
#
# Usage:
#   ./retrigger_aws_cnr_imports.sh
#
# Features:
#   - Queries all AWS CNR datasources from the database
#   - Supports inclusion list (if not empty, only these datasources are processed)
#   - Supports exclusion list (ignored if inclusion list is not empty)
#   - Accepts time input for when to trigger reimport (YYYY-MM-DD format)
#   - Uses the existing retrigger_imports.sh script internally
#
##############################################################################

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Retrigger AWS CNR Imports${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check if retrigger_imports.sh exists
if [ ! -f "retrigger_imports.sh" ]; then
    echo -e "${RED}Error: retrigger_imports.sh not found in current directory!${NC}"
    echo "Please run this script from the OptScale repository root."
    exit 1
fi

# Database configuration
USER_CONFIG="optscale-deploy/overlay/user_config.yml"
if [ ! -f "$USER_CONFIG" ]; then
    echo -e "${RED}Error: $USER_CONFIG not found!${NC}"
    echo "Please run this script from the OptScale repository root."
    exit 1
fi

MYSQL_HOST="mariadb"
MYSQL_PORT="3306"
MYSQL_USER="root"
MYSQL_PASSWORD="my-password-01"
MYSQL_DB="my-db"
MYSQL_BIN="/opt/homebrew/opt/mysql-client/bin/mysql"

# Inclusion list - if not empty, ONLY these datasources will be processed
# Edit this list to add/remove datasources to include
# When inclusion list is not empty, exclusion list is ignored
INCLUSION_LIST=(
    "citizens_prod_infra"
    "dbai-rnd"
    "devtest-infra"
    "forbes_byoa_1"
    "JD"
    "native-cp-rnd"
    "ops-production"
    "staging-infra"
    "starship-dev"
    "starship-prod"
    "tessell-ops"
    "tessell-poc-dp"
)

# Exclusion list - datasources with these names will be skipped
# NOTE: This list is IGNORED when INCLUSION_LIST is not empty
# Edit this list to add/remove datasources to exclude
EXCLUSION_LIST=(
    "citizens_dbservices_0"
    "Tessell"
    "canary-byoa-0"
    "qa-dataplane-byoa-3"
    "finops"
    "tessell_dba_0"
)

# Display configured lists
if [ ${#INCLUSION_LIST[@]} -gt 0 ]; then
    echo -e "${GREEN}Inclusion list configured (only these will be processed):${NC}"
    for included in "${INCLUSION_LIST[@]}"; do
        echo "  - $included"
    done
    echo -e "${YELLOW}Note: Exclusion list is ignored when inclusion list is not empty${NC}"
else
    echo -e "${YELLOW}Inclusion list: (empty - using exclusion list mode)${NC}"
    echo -e "${YELLOW}Exclusion list configured:${NC}"
    if [ ${#EXCLUSION_LIST[@]} -eq 0 ]; then
        echo "  (none - all AWS CNR datasources will be processed)"
    else
        for excluded in "${EXCLUSION_LIST[@]}"; do
            echo "  - $excluded"
        done
    fi
fi
echo ""

# Get reimport date from user
echo -e "${YELLOW}Enter the date from which to trigger reimport (YYYY-MM-DD format):${NC}"
echo -e "${YELLOW}Example: 2026-04-01${NC}"
read -p "Date: " FROM_DATE

# Validate date format
if [ -z "$FROM_DATE" ]; then
    echo -e "${RED}Error: Date is required${NC}"
    exit 1
fi

if ! [[ "$FROM_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo -e "${RED}Error: Invalid date format. Use YYYY-MM-DD (e.g., 2026-04-01)${NC}"
    exit 1
fi

echo ""
echo -e "${YELLOW}Testing database connection...${NC}"
if ! $MYSQL_BIN -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DB" -e "SELECT 1;" &> /dev/null; then
    echo -e "${RED}Error: Could not connect to MariaDB${NC}"
    echo "Make sure kubefwd is running: ${BLUE}sudo kubefwd svc${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Connected${NC}"

echo ""
echo -e "${YELLOW}Fetching AWS CNR datasources...${NC}"

# Query all AWS CNR cloud accounts that are not deleted
AWS_ACCOUNTS=$($MYSQL_BIN -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DB" -N -e \
    "SELECT id, name FROM cloudaccount WHERE deleted_at = 0 AND type = 'aws_cnr' ORDER BY name;")

if [ -z "$AWS_ACCOUNTS" ]; then
    echo -e "${RED}Error: No AWS CNR datasources found${NC}"
    exit 1
fi

# Parse the results and filter based on inclusion/exclusion list
declare -a ACCOUNT_IDS
declare -a ACCOUNT_NAMES
declare -a EXCLUDED_ACCOUNTS
declare -a NOT_FOUND_IN_INCLUSION

# If inclusion list is not empty, use inclusion mode
if [ ${#INCLUSION_LIST[@]} -gt 0 ]; then
    # Inclusion mode: only process datasources in the inclusion list
    while IFS=$'\t' read -r id name; do
        INCLUDED=false
        for included_name in "${INCLUSION_LIST[@]}"; do
            if [ "$name" = "$included_name" ]; then
                INCLUDED=true
                break
            fi
        done

        if [ "$INCLUDED" = true ]; then
            ACCOUNT_IDS+=("$id")
            ACCOUNT_NAMES+=("$name")
        fi
    done <<< "$AWS_ACCOUNTS"

    # Check if any datasources from inclusion list were not found
    for included_name in "${INCLUSION_LIST[@]}"; do
        FOUND=false
        for account_name in "${ACCOUNT_NAMES[@]}"; do
            if [ "$account_name" = "$included_name" ]; then
                FOUND=true
                break
            fi
        done
        if [ "$FOUND" = false ]; then
            NOT_FOUND_IN_INCLUSION+=("$included_name")
        fi
    done
else
    # Exclusion mode: process all except those in the exclusion list
    while IFS=$'\t' read -r id name; do
        EXCLUDED=false
        for excluded_name in "${EXCLUSION_LIST[@]}"; do
            if [ "$name" = "$excluded_name" ]; then
                EXCLUDED=true
                EXCLUDED_ACCOUNTS+=("$name")
                break
            fi
        done

        if [ "$EXCLUDED" = false ]; then
            ACCOUNT_IDS+=("$id")
            ACCOUNT_NAMES+=("$name")
        fi
    done <<< "$AWS_ACCOUNTS"
fi

TOTAL_COUNT=${#ACCOUNT_IDS[@]}
EXCLUDED_COUNT=${#EXCLUDED_ACCOUNTS[@]}
NOT_FOUND_COUNT=${#NOT_FOUND_IN_INCLUSION[@]}

echo -e "${GREEN}✓ Found $(echo "$AWS_ACCOUNTS" | wc -l | tr -d ' ') total AWS CNR datasource(s)${NC}"

if [ ${#INCLUSION_LIST[@]} -gt 0 ]; then
    # Inclusion mode reporting
    echo -e "${GREEN}  Matched in inclusion list: $TOTAL_COUNT${NC}"
    if [ $NOT_FOUND_COUNT -gt 0 ]; then
        echo -e "${YELLOW}  Not found in database: $NOT_FOUND_COUNT${NC}"
    fi
else
    # Exclusion mode reporting
    echo -e "${YELLOW}  Excluded: $EXCLUDED_COUNT${NC}"
    echo -e "${GREEN}  To process: $TOTAL_COUNT${NC}"
fi

# Show excluded datasources (only in exclusion mode)
if [ ${#INCLUSION_LIST[@]} -eq 0 ] && [ $EXCLUDED_COUNT -gt 0 ]; then
    echo ""
    echo -e "${YELLOW}Excluded datasources:${NC}"
    for excluded in "${EXCLUDED_ACCOUNTS[@]}"; do
        echo "  - $excluded"
    done
fi

# Show not found datasources (only in inclusion mode)
if [ ${#INCLUSION_LIST[@]} -gt 0 ] && [ $NOT_FOUND_COUNT -gt 0 ]; then
    echo ""
    echo -e "${YELLOW}Datasources in inclusion list not found in database:${NC}"
    for not_found in "${NOT_FOUND_IN_INCLUSION[@]}"; do
        echo "  - $not_found"
    done
fi

if [ $TOTAL_COUNT -eq 0 ]; then
    echo ""
    if [ ${#INCLUSION_LIST[@]} -gt 0 ]; then
        echo -e "${YELLOW}No AWS CNR datasources to process (none matched inclusion list)${NC}"
    else
        echo -e "${YELLOW}No AWS CNR datasources to process (all excluded or none found)${NC}"
    fi
    exit 0
fi

echo ""
echo -e "${YELLOW}Datasources to process:${NC}"
for i in "${!ACCOUNT_IDS[@]}"; do
    echo "  $((i+1)). ${ACCOUNT_NAMES[$i]} (${ACCOUNT_IDS[$i]})"
done

echo ""
echo -e "${YELLOW}This will retrigger imports for $TOTAL_COUNT AWS CNR datasource(s) from $FROM_DATE${NC}"
echo -e "${YELLOW}Each datasource will be processed using retrigger_imports.sh${NC}"
read -p "Continue? (yes/no): " CONFIRM
if [ "$CONFIRM" != "yes" ]; then
    echo -e "${YELLOW}Aborted${NC}"
    exit 0
fi

echo ""
echo -e "${YELLOW}Processing datasources...${NC}"
echo ""

SUCCESS_COUNT=0
FAIL_COUNT=0
declare -a FAILED_ACCOUNTS

for i in "${!ACCOUNT_IDS[@]}"; do
    ACCOUNT_ID="${ACCOUNT_IDS[$i]}"
    ACCOUNT_NAME="${ACCOUNT_NAMES[$i]}"

    echo -e "${BLUE}[$((i+1))/$TOTAL_COUNT] Processing: $ACCOUNT_NAME${NC}"
    echo -e "${BLUE}             ID: $ACCOUNT_ID${NC}"

    # Call retrigger_imports.sh for this account
    # Provide empty input to auto-confirm kubefwd prompt and "yes" for final confirmation
    if echo -e "\nyes" | ./retrigger_imports.sh "$ACCOUNT_ID" "$FROM_DATE" 2>/dev/null; then
        echo -e "${GREEN}✓ Success${NC}"
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    else
        echo -e "${RED}✗ Failed${NC}"
        FAIL_COUNT=$((FAIL_COUNT + 1))
        FAILED_ACCOUNTS+=("$ACCOUNT_NAME ($ACCOUNT_ID)")
    fi

    echo ""

    # Small delay between accounts to avoid overwhelming the system
    if [ $i -lt $((TOTAL_COUNT - 1)) ]; then
        sleep 2
    fi
done

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Summary${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "${YELLOW}Mode: $([ ${#INCLUSION_LIST[@]} -gt 0 ] && echo "Inclusion list" || echo "Exclusion list")${NC}"
echo -e "${YELLOW}Reimport date: $FROM_DATE${NC}"
echo -e "${YELLOW}Total AWS CNR datasources found: $(echo "$AWS_ACCOUNTS" | wc -l | tr -d ' ')${NC}"
if [ ${#INCLUSION_LIST[@]} -gt 0 ]; then
    echo -e "${YELLOW}Matched in inclusion list: $TOTAL_COUNT${NC}"
    if [ $NOT_FOUND_COUNT -gt 0 ]; then
        echo -e "${YELLOW}Not found in database: $NOT_FOUND_COUNT${NC}"
    fi
else
    echo -e "${YELLOW}Excluded: $EXCLUDED_COUNT${NC}"
    echo -e "${YELLOW}Processed: $TOTAL_COUNT${NC}"
fi
echo ""
echo -e "${GREEN}Successful: $SUCCESS_COUNT${NC}"
if [ $FAIL_COUNT -gt 0 ]; then
    echo -e "${RED}Failed: $FAIL_COUNT${NC}"
    echo ""
    echo -e "${YELLOW}Failed datasources:${NC}"
    for failed in "${FAILED_ACCOUNTS[@]}"; do
        echo "  - $failed"
    done
fi

echo ""
echo -e "${YELLOW}Next Steps:${NC}"
echo -e "  1. Monitor import progress: ${BLUE}kubectl logs -l app=diworker -f${NC}"
echo -e "  2. Check import status in MariaDB (see mariadb-debug.sql)"
echo -e "  3. Data will be re-imported from $FROM_DATE onwards"
echo ""
echo -e "${GREEN}Done!${NC}"

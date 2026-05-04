#!/bin/bash

##############################################################################
# Description: Manually trigger imports for all cloud accounts
#
# Usage:
#   ./retrigger_imports.sh [ACCOUNT_ID] [FROM_DATE]
#
# Examples:
#   ./retrigger_imports.sh                           # Trigger normal import for all accounts
#   ./retrigger_imports.sh abc123                    # Trigger normal import for specific account
#   ./retrigger_imports.sh abc123 2026-04-01         # Reset import to April 1st, 2026 and trigger
#   ./retrigger_imports.sh all 2026-04-01            # Reset all accounts to April 1st, 2026
#
##############################################################################

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Trigger Manual Imports${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

ACCOUNT_ID=${1}
FROM_DATE=${2}


USER_CONFIG="optscale-deploy/overlay/user_config.yml"
if [ ! -f "$USER_CONFIG" ]; then
    echo -e "${RED}Error: $USER_CONFIG not found!${NC}"
    echo "Please run this script from the OptScale repository root."
    exit 1
fi

echo -e "${YELLOW}Reading cluster secret...${NC}"
CLUSTER_SECRET=$(grep -A1 "^secrets:" "$USER_CONFIG" | grep "cluster:" | awk '{print $2}')

if [ -z "$CLUSTER_SECRET" ]; then
    echo -e "${RED}Error: Could not find cluster secret in $USER_CONFIG${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Cluster secret found${NC}"

echo ""
echo -e "${YELLOW}===========================================\n"
echo -e "This script requires 'kubefwd' to be running.\n"
echo -e "Please run in a separate terminal:\n"
echo -e "  ${BLUE}sudo kubefwd svc${NC}\n"
echo -e "===========================================\n"
echo -e "${NC}"

read -p "Press Enter once kubefwd is running and services are accessible..."

MYSQL_HOST="mariadb"
MYSQL_PORT="3306"
MYSQL_USER="root"
MYSQL_PASSWORD="my-password-01"
MYSQL_DB="my-db"

REST_API_URL="http://restapi:80"
MYSQL_BIN="/opt/homebrew/opt/mysql-client/bin/mysql"

echo ""
echo -e "${YELLOW}MariaDB: $MYSQL_USER@$MYSQL_HOST:$MYSQL_PORT/$MYSQL_DB${NC}"
echo -e "${YELLOW}REST API: $REST_API_URL${NC}"

# Validate FROM_DATE if provided
FROM_DATE_TIMESTAMP=""
if [ -n "$FROM_DATE" ]; then
    echo ""
    echo -e "${YELLOW}Date-based import requested: $FROM_DATE${NC}"

    # Validate date format (YYYY-MM-DD)
    if ! [[ "$FROM_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
        echo -e "${RED}Error: Invalid date format. Use YYYY-MM-DD (e.g., 2026-04-01)${NC}"
        exit 1
    fi

    # Convert date to Unix timestamp using MySQL
    FROM_DATE_TIMESTAMP=$($MYSQL_BIN -h $MYSQL_HOST -P $MYSQL_PORT -u $MYSQL_USER -p$MYSQL_PASSWORD -D $MYSQL_DB -N -s -e "SELECT UNIX_TIMESTAMP('$FROM_DATE 00:00:00');")

    if [ -z "$FROM_DATE_TIMESTAMP" ] || [ "$FROM_DATE_TIMESTAMP" = "NULL" ]; then
        echo -e "${RED}Error: Failed to convert date to timestamp${NC}"
        exit 1
    fi

    echo -e "${GREEN}✓ Date validated: $FROM_DATE (timestamp: $FROM_DATE_TIMESTAMP)${NC}"
    echo ""
    echo -e "${YELLOW}WARNING: This will reset the following timestamps for selected account(s):${NC}"
    echo -e "${YELLOW}  - last_import_at → $FROM_DATE_TIMESTAMP${NC}"
    echo -e "${YELLOW}  - last_import_modified_at → $FROM_DATE_TIMESTAMP${NC}"
    echo -e "${YELLOW}  - last_import_attempt_at → 0${NC}"
    echo -e "${YELLOW}  - last_import_attempt_error → NULL${NC}"
    echo ""
    echo -e "${YELLOW}This will cause OptScale to re-import data from $FROM_DATE onwards.${NC}"
    echo -e "${YELLOW}For AWS: Will fetch ~5 days of data from this date${NC}"
    echo -e "${YELLOW}For GCP: Will fetch from last expense date - 3 days${NC}"
    echo -e "${YELLOW}For Azure: Will fetch ~1 day of data from this date${NC}"
    echo ""
fi

echo ""
echo -e "${YELLOW}Testing database connection...${NC}"
if ! $MYSQL_BIN -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DB" -e "SELECT 1;" &> /dev/null; then
    echo -e "${RED}Error: Could not connect to MariaDB${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Connected${NC}"

echo ""
echo -e "${YELLOW}Fetching cloud accounts...${NC}"

CLOUD_ACCOUNT_IDS=$($MYSQL_BIN -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DB" -N -e \
    "SELECT id FROM cloudaccount WHERE id='$ACCOUNT_ID';")

if [ -z "$CLOUD_ACCOUNT_IDS" ]; then
    echo -e "${RED}Error: No cloud accounts found${NC}"
    exit 1
fi

ACCOUNT_COUNT=$(echo "$CLOUD_ACCOUNT_IDS" | wc -l | tr -d ' ')
echo -e "${GREEN}✓ Found $ACCOUNT_COUNT cloud account(s)${NC}"

echo ""
if [ -n "$FROM_DATE" ]; then
    echo -e "${YELLOW}This will:${NC}"
    echo -e "${YELLOW}  1. Reset import timestamps to $FROM_DATE for $ACCOUNT_COUNT cloud account(s)${NC}"
    echo -e "${YELLOW}  2. Trigger manual imports to re-fetch data from that date${NC}"
    echo -e "${YELLOW}  3. Existing data in MongoDB/ClickHouse will be updated (not deleted)${NC}"
else
    echo -e "${YELLOW}This will trigger normal manual import for $ACCOUNT_COUNT cloud account(s).${NC}"
fi
read -p "Continue? (yes/no): " CONFIRM
if [ "$CONFIRM" != "yes" ]; then
    echo -e "${YELLOW}Aborted${NC}"
    exit 0
fi

echo ""

# Step 1: Reset timestamps if FROM_DATE is provided
if [ -n "$FROM_DATE" ]; then
    echo -e "${YELLOW}Step 1: Resetting import timestamps to $FROM_DATE...${NC}"
    echo ""

    RESET_COUNT=0
    for CA_ID in $CLOUD_ACCOUNT_IDS; do
        echo -n "$CA_ID... "

        # Get cloud account type for display
        CA_TYPE=$($MYSQL_BIN -h $MYSQL_HOST -P $MYSQL_PORT -u $MYSQL_USER -p$MYSQL_PASSWORD -D $MYSQL_DB -N -s -e "SELECT type FROM cloudaccount WHERE id='$CA_ID' AND deleted_at=0;")

        # Reset the import timestamps
        $MYSQL_BIN -h $MYSQL_HOST -P $MYSQL_PORT -u $MYSQL_USER -p$MYSQL_PASSWORD -D $MYSQL_DB -e "
            UPDATE cloudaccount
            SET
                last_import_at = $FROM_DATE_TIMESTAMP,
                last_import_modified_at = $FROM_DATE_TIMESTAMP,
                last_import_attempt_at = 0,
                last_import_attempt_error = NULL
            WHERE id = '$CA_ID'
              AND deleted_at = 0;
        " 2>/dev/null

        if [ $? -eq 0 ]; then
            echo -e "${GREEN}✓ Reset ($CA_TYPE)${NC}"
            RESET_COUNT=$((RESET_COUNT + 1))
        else
            echo -e "${RED}✗ Failed to reset${NC}"
        fi
    done

    echo ""
    echo -e "${GREEN}✓ Reset $RESET_COUNT cloud account(s)${NC}"
    echo ""

    # Small delay to ensure DB updates are flushed
    sleep 1
fi

# Step 2: Trigger imports
if [ -n "$FROM_DATE" ]; then
    echo -e "${YELLOW}Step 2: Triggering imports...${NC}"
else
    echo -e "${YELLOW}Triggering imports...${NC}"
fi
echo ""

SUCCESS_COUNT=0
FAIL_COUNT=0
FAILED_ACCOUNTS=()

for CA_ID in $CLOUD_ACCOUNT_IDS; do
    echo -n "$CA_ID... "

    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
        -H "Content-Type: application/json" \
        -H "Secret: $CLUSTER_SECRET" \
        -d "{\"cloud_account_id\": \"$CA_ID\"}" \
        "$REST_API_URL/restapi/v2/schedule_imports")

    if [ "$HTTP_CODE" = "201" ]; then
        echo -e "${GREEN}✓${NC}"
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    else
        echo -e "${RED}✗ (HTTP $HTTP_CODE)${NC}"
        FAIL_COUNT=$((FAIL_COUNT + 1))
        FAILED_ACCOUNTS+=("$CA_ID")
    fi

    sleep 0.3
done

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Summary${NC}"
echo -e "${BLUE}========================================${NC}"

if [ -n "$FROM_DATE" ]; then
    echo -e "${YELLOW}Import reset to: $FROM_DATE${NC}"
    echo ""
fi

echo -e "${GREEN}Imports triggered: $SUCCESS_COUNT${NC}"
if [ $FAIL_COUNT -gt 0 ]; then
    echo -e "${RED}Failed: $FAIL_COUNT${NC}"
    echo ""
    echo -e "${YELLOW}Failed IDs:${NC}"
    for FAILED_ID in "${FAILED_ACCOUNTS[@]}"; do
        echo "  $FAILED_ID"
    done
fi

echo ""
if [ -n "$FROM_DATE" ]; then
    echo -e "${YELLOW}Next Steps:${NC}"
    echo -e "  1. Monitor import progress: ${BLUE}kubectl logs -l app=diworker -f${NC}"
    echo -e "  2. Check import status in MariaDB (see mariadb-debug.sql)"
    echo -e "  3. Data will be re-imported from $FROM_DATE onwards"
    echo ""
fi
echo -e "${GREEN}Done!${NC}"

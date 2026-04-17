#!/bin/bash

##############################################################################
# Description: Manually trigger imports for all cloud accounts
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
echo -e "${GREEN}✓ Found $ACCOUNT_COUNT cloud account ${NC}"

echo ""
echo -e "${YELLOW}This will trigger manual import for $ACCOUNT_COUNT cloud account.${NC}"
read -p "Continue? (yes/no): " CONFIRM
if [ "$CONFIRM" != "yes" ]; then
    echo -e "${YELLOW}Aborted${NC}"
    exit 0
fi

echo ""
echo -e "${YELLOW}Triggering imports...${NC}"
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
echo -e "${GREEN}Success: $SUCCESS_COUNT${NC}"
if [ $FAIL_COUNT -gt 0 ]; then
    echo -e "${RED}Failed: $FAIL_COUNT${NC}"
    echo ""
    echo -e "${YELLOW}Failed IDs:${NC}"
    for FAILED_ID in "${FAILED_ACCOUNTS[@]}"; do
        echo "  $FAILED_ID"
    done
fi
echo ""
echo -e "${GREEN}Done!${NC}"

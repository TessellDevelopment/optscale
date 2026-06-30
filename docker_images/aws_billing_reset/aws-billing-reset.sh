#!/bin/sh
# aws-billing-reset: monthly rolling-window reset for AWS datasources.
#
# Runs on the 10th of each month (after AWS finalises the prior month's bill).
# Resets last_import_modified_at to (now - AWS_BILLING_RESET_DAYS days) for
# every active aws_cnr account so that the next scheduled diworker import
# re-fetches any S3 CUR files whose LastModified has advanced since the
# original import (retroactive credits, EDP true-ups, marketplace charges).
#
# No raw_expenses or ClickHouse data is touched here; the reset only controls
# which S3 files diworker will re-download on its next run.
set -eu

function mysql_cmd() {
    MYSQL_PWD="${DB_PASSWORD}" mysql \
        -h "${DB_HOST}" \
        -P "${DB_PORT}" \
        -u "${DB_USER}" \
        --batch \
        --skip-column-names \
        "${DB_NAME}" \
        "$@"
}

CUTOFF=$(( $(date +%s) - AWS_BILLING_RESET_DAYS * 86400 ))

echo "[aws-billing-reset] START at $(date -u +%FT%TZ)"
echo "[aws-billing-reset] Cutoff: ${CUTOFF} (-${AWS_BILLING_RESET_DAYS} days)"

NAMES=$(mysql_cmd -e "
    SELECT name FROM cloudaccount
    WHERE  deleted_at              = 0
      AND  type                    = 'aws_cnr'
      AND  auto_import             = 1
      AND  last_import_at          > 0
      AND  last_import_modified_at > ${CUTOFF};
")

if [ -z "${NAMES}" ]; then
    echo "[aws-billing-reset] No AWS accounts require a billing reset."
    exit 0
fi

COUNT=$(printf '%s\n' "${NAMES}" | wc -l | tr -d ' \t')
NAMES_CSV=$(printf '%s' "${NAMES}" | tr '\n' ',' | sed 's/,$//')
echo "[aws-billing-reset] Resetting ${COUNT} account(s): ${NAMES_CSV}"

mysql_cmd -e "
    UPDATE cloudaccount
    SET    last_import_modified_at = ${CUTOFF}
    WHERE  deleted_at              = 0
      AND  type                    = 'aws_cnr'
      AND  auto_import             = 1
      AND  last_import_at          > 0
      AND  last_import_modified_at > ${CUTOFF};
"

echo "[aws-billing-reset] Done — ${COUNT} account(s) updated."

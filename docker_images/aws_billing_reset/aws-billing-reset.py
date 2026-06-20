"""
aws-billing-reset: monthly rolling-window reset for AWS datasources.

Runs on the 10th of each month (after AWS finalises the prior month's bill).
Resets last_import_modified_at to (now - 60 days) for every active aws_cnr
account so that the next scheduled diworker import re-fetches any S3 CUR files
whose LastModified has advanced since the original import (retroactive credits,
EDP true-ups, marketplace charges, etc.).

The 60-day window is intentional:
  - EDP / enterprise credits: typically applied within 30-45 days
  - Marketplace charges: up to 45 days late
  - Month-end AWS bill finalisation: already handled automatically by
    diworker's _is_first_import_in_month, but 60 days adds a safety buffer

No raw_expenses or ClickHouse data is touched here; the reset only controls
which S3 files diworker will re-download on its next run.
"""
import logging
import os
import time

import etcd
from retrying import retry
from sqlalchemy import create_engine, text
from optscale_client.config_client.client import Client as ConfigClient

LOG = logging.getLogger(__name__)

RESET_DAYS = int(os.environ.get('AWS_BILLING_RESET_DAYS', 60))
DEFAULT_RETRY_ARGS = dict(stop_max_attempt_number=10, wait_fixed=2000)


def _retry_on_etcd(exc):
    return isinstance(exc, etcd.EtcdKeyNotFound)


class AWSBillingReset:
    def __init__(self):
        self._config_cl = None
        self._engine = None

    @property
    def config_cl(self):
        if not self._config_cl:
            self._config_cl = ConfigClient(
                host=os.environ['HX_ETCD_HOST'],
                port=int(os.environ['HX_ETCD_PORT']),
            )
        return self._config_cl

    @retry(**DEFAULT_RETRY_ARGS, retry_on_exception=_retry_on_etcd)
    def _get_db_params(self):
        return self.config_cl.rest_db_params()

    @property
    def engine(self):
        if not self._engine:
            user, password, host, db = self._get_db_params()
            self._engine = create_engine(
                f'mysql+mysqlconnector://{user}:{password}@{host}/{db}'
                f'?charset=utf8mb4',
                pool_pre_ping=True,
            )
        return self._engine

    def run(self):
        cutoff_ts = int(time.time()) - RESET_DAYS * 86400

        with self.engine.connect() as conn:
            # Fetch accounts that have already been imported at least once
            # (last_import_at > 0) and whose current cursor is older than the
            # reset window — i.e., they actually have stale data to refresh.
            rows = conn.execute(text("""
                SELECT id, name
                FROM cloudaccount
                WHERE deleted_at = 0
                  AND type        = 'aws_cnr'
                  AND auto_import = 1
                  AND last_import_at > 0
                  AND last_import_modified_at > :cutoff
            """), {'cutoff': cutoff_ts}).fetchall()

            if not rows:
                LOG.info('No AWS accounts require a billing reset.')
                return

            ids = [r[0] for r in rows]
            LOG.info(
                'Resetting last_import_modified_at to -%d days for %d '
                'account(s): %s',
                RESET_DAYS, len(ids),
                ', '.join(r[1] for r in rows),
            )

            conn.execute(text("""
                UPDATE cloudaccount
                SET    last_import_modified_at = :cutoff
                WHERE  id IN :ids
            """), {'cutoff': cutoff_ts, 'ids': tuple(ids)})

        LOG.info('Billing reset complete — %d account(s) updated.', len(ids))


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
    )
    config_cl = ConfigClient(
        host=os.environ['HX_ETCD_HOST'],
        port=int(os.environ['HX_ETCD_PORT']),
    )
    config_cl.wait_configured()
    AWSBillingReset().run()

--- Login to mariadb
 mysql -u root -pmy-password-01 my-db

--- Check and Purge rabbitmq queues
rabbitmqadmin list queues name messages messages_ready messages_unacknowledged
rabbitmqadmin purge queue name=report-import

-- Check how old the oldest imports are
SELECT 
    MIN(FROM_UNIXTIME(created_at)) as oldest,
    TIMESTAMPDIFF(DAY, FROM_UNIXTIME(MIN(created_at)), NOW()) as days_old
FROM reportimport 
WHERE deleted_at = 0 AND state IN ('scheduled', 'in_progress');

-- Mark old stuck imports as failed (older than 24 hours)
UPDATE reportimport 
SET 
    state = 'failed',
    state_reason = 'Import timed out - exceeded processing threshold',
    updated_at = UNIX_TIMESTAMP(NOW())
WHERE deleted_at = 0 
  AND state IN ('scheduled', 'in_progress');

-- Check import states
SELECT state, COUNT(*) FROM reportimport WHERE deleted_at = 0 GROUP BY state;

-- Enable auto_import for all real cloud accounts
UPDATE cloudaccount
SET auto_import = 1
WHERE deleted_at = 0 and type in ('aws_cnr', 'gcp_cnr', 'azure_cnr') ;

-- Disable auto_import for virutal cloud account
UPDATE cloudaccount 
SET auto_import = 0
WHERE deleted_at = 0 and id = '00000000-0000-0000-0000-000000000000' ;

-- Mark stuck imports as failed
UPDATE reportimport 
SET 
    state = 'failed',
    state_reason = 'Cleared due to credential rotation',
    updated_at = UNIX_TIMESTAMP(NOW())
WHERE deleted_at = 0 
  AND state IN ('scheduled', 'in_progress');


-- Get all cloud accounts that have not been imported in the last 5 days
SELECT
    name,
    type,
    DATEDIFF(CURDATE(),
        CASE
            WHEN last_import_at = 0 OR last_import_modified_at = 0
                THEN DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 3 MONTH), '%Y-%m-01')
            WHEN type = 'aws_cnr'
             AND ((MONTH(FROM_UNIXTIME(last_import_modified_at)) + 1 = MONTH(NOW()) AND YEAR(FROM_UNIXTIME(last_import_modified_at)) = YEAR(NOW()))
               OR (MONTH(NOW()) = 1 AND YEAR(FROM_UNIXTIME(last_import_modified_at)) + 1 = YEAR(NOW())))
                THEN DATE_FORMAT(FROM_UNIXTIME(last_import_modified_at), '%Y-%m-01')
            ELSE DATE_SUB(FROM_UNIXTIME(last_import_modified_at), INTERVAL 5 DAY)
        END
    ) AS days_to_import
FROM cloudaccount
WHERE deleted_at = 0
ORDER BY days_to_import DESC, type, name;

-- Get all cloud accounts
SELECT id,name,type,auto_import from cloudaccount where deleted_at=0;

--- Create GCP-UNATTACHED datasource
INSERT INTO cloudaccount (
    id,
    created_at,
    deleted_at,
    name,
    type,
    config,
    organization_id,
    account_id,
    auto_import,
    import_period,
    last_import_at,
    last_import_modified_at,
    last_import_attempt_at,
    last_getting_metrics_at,
    last_getting_metric_attempt_at,
    cleaned_at,
    process_recommendations
) VALUES (
    '00000000-0000-0000-0000-000000000000',
    UNIX_TIMESTAMP(),
    0,
    'GCP-UNATTACHED (Virtual)',
    'gcp_cnr',
    '<ENCRYPTED_GCP_CONFIG_FROM_ANY_GCP_DATASOURCE>',
    'f4515e3f-5a6f-47fd-b137-4b8dd783b9bf',
    'GCP-UNATTACHED',
    1,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0
);
--- Check all scheduled and progressing datasources for report import
SELECT
    ca.name        AS datasource_name,
    ca.type        AS datasource_type,
    ri.state       AS import_state
FROM reportimport ri
JOIN cloudaccount ca ON ca.id = ri.cloud_account_id
WHERE ri.deleted_at = 0
  AND ca.deleted_at = 0
  AND ri.state IN ('scheduled', 'in_progress')
ORDER BY ri.state, ca.name;

---
 mysql -u root -pmy-password-01 my-db
---
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

-- Disable auto_import for all cloud accounts
UPDATE cloudaccount 
SET auto_import = 0
WHERE deleted_at = 0 and type in ('aws_cnr', 'gcp_cnr', 'azure_cnr') ;

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
    id,
    name,
    type,
    FROM_UNIXTIME(last_import_modified_at) as last_modified,
    DATEDIFF(NOW(), FROM_UNIXTIME(last_import_modified_at)) as days_gap,
    CASE 
        WHEN type = 'AWS_CNR' THEN CONCAT(
            'Will import from ',
            DATE_FORMAT(DATE_SUB(FROM_UNIXTIME(last_import_modified_at), INTERVAL 5 DAY), '%Y-%m-%d'),
            ' to ',
            CURDATE(),
            ' (~', 
            DATEDIFF(CURDATE(), DATE_SUB(FROM_UNIXTIME(last_import_modified_at), INTERVAL 5 DAY)),
            ' days)'
        )
        WHEN type = 'AZURE_CNR' THEN CONCAT(
            'Will import from ',
            DATE_FORMAT(DATE_SUB(FROM_UNIXTIME(last_import_modified_at), INTERVAL 5 DAY), '%Y-%m-%d'),
            ' to ',
            CURDATE(),
            ' (~',
            DATEDIFF(CURDATE(), DATE_SUB(FROM_UNIXTIME(last_import_modified_at), INTERVAL 5 DAY)),
            ' days)'
        )
        WHEN type = 'GCP_CNR' THEN 'Will import last_expense_date - 3 days to today'
    END as import_range_description
FROM cloudaccount 
WHERE deleted_at = 0
ORDER BY type, last_import_modified_at;

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
    '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*ZrMi0ol/VTztad3EHgxtlQ==*p8fxRKv+i3iTqbRFcp8/IA==*PDcC9r7gmlKCN+ufJoTCmg==',
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

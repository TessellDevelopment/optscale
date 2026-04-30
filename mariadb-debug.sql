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

-- =============================================================================
-- Snowflake setup for nse-streaming-pipeline
--
-- Run as ACCOUNTADMIN, top to bottom. The account must be on AWS in the same
-- region as the raw S3 bucket (here: AWS Asia Pacific (Singapore)), because
-- S3 can only send event notifications to an SQS queue in its own region.
--
-- Values that must match terraform/main.tf:
--   AWS account ID ......... 163649804794
--   Raw bucket ............. nse-stream-raw-rituraj-sg-4821
--   Snowflake read role .... snowflake-s3-role
-- Change them in both places when deploying to another account.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- -----------------------------------------------------------------------------
-- 1. Compute and storage
-- -----------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS NSE_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60            -- seconds idle before suspending
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS NSE_STREAM;
CREATE SCHEMA IF NOT EXISTS NSE_STREAM.RAW;

-- -----------------------------------------------------------------------------
-- 2. Role used by dbt (least privilege: read RAW, build its own schemas)
-- -----------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS TRANSFORM;
GRANT ROLE TRANSFORM TO ROLE SYSADMIN;

GRANT USAGE, OPERATE ON WAREHOUSE NSE_WH TO ROLE TRANSFORM;
GRANT USAGE, CREATE SCHEMA ON DATABASE NSE_STREAM TO ROLE TRANSFORM;
GRANT USAGE ON SCHEMA NSE_STREAM.RAW TO ROLE TRANSFORM;
GRANT SELECT ON FUTURE TABLES IN SCHEMA NSE_STREAM.RAW TO ROLE TRANSFORM;

-- Let your own login use the role too
GRANT ROLE TRANSFORM TO USER <YOUR_SNOWFLAKE_USER>;

-- -----------------------------------------------------------------------------
-- 3. Storage integration: Snowflake assumes an AWS role to read the bucket
-- -----------------------------------------------------------------------------
CREATE STORAGE INTEGRATION IF NOT EXISTS S3_NSE_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::163649804794:role/snowflake-s3-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://nse-stream-raw-rituraj-sg-4821/raw/');

-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID into the
-- snowflake_iam_user_arn and snowflake_external_id locals in terraform/main.tf.
-- Terraform puts them in the trust policy of snowflake-s3-role.
DESC INTEGRATION S3_NSE_INT;

-- -----------------------------------------------------------------------------
-- 4. File format and external stage over raw/stocks/
-- -----------------------------------------------------------------------------
CREATE FILE FORMAT IF NOT EXISTS NSE_STREAM.RAW.JSONL_FORMAT
  TYPE = JSON;                 -- one JSON object per line (JSON Lines)

CREATE STAGE IF NOT EXISTS NSE_STREAM.RAW.S3_NSE_STAGE
  STORAGE_INTEGRATION = S3_NSE_INT
  URL = 's3://nse-stream-raw-rituraj-sg-4821/raw/stocks/'
  FILE_FORMAT = NSE_STREAM.RAW.JSONL_FORMAT;

-- -----------------------------------------------------------------------------
-- 5. Raw landing table: JSON kept untouched (schema-on-read) + load metadata
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS NSE_STREAM.RAW.STOCK_CANDLES_RAW (
  RECORD          VARIANT,         -- the full JSON row as it arrived
  SOURCE_FILE     STRING,          -- S3 key it came from
  FILE_ROW_NUMBER NUMBER,          -- line number inside that file
  LOADED_AT       TIMESTAMP_LTZ    -- when Snowpipe loaded it
);

-- -----------------------------------------------------------------------------
-- 6. Snowpipe with auto-ingest (S3 event -> SQS -> COPY INTO)
-- -----------------------------------------------------------------------------
CREATE PIPE IF NOT EXISTS NSE_STREAM.RAW.STOCK_CANDLES_PIPE
  AUTO_INGEST = TRUE
AS
  COPY INTO NSE_STREAM.RAW.STOCK_CANDLES_RAW (RECORD, SOURCE_FILE, FILE_ROW_NUMBER, LOADED_AT)
  FROM (
    SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME
    FROM @NSE_STREAM.RAW.S3_NSE_STAGE
  );

-- Copy the notification_channel value (an SQS ARN) into the
-- snowpipe_queue_arn local in terraform/main.tf. Terraform points the
-- bucket's ObjectCreated notification at it.
SHOW PIPES IN SCHEMA NSE_STREAM.RAW;

-- -----------------------------------------------------------------------------
-- 7. Service user for dbt Cloud (key-pair login only, no password)
-- -----------------------------------------------------------------------------
-- Generate an encrypted PKCS#8 key pair first, for example:
--   openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 aes256 -inform PEM -out dbt_rsa_key.p8
--   openssl rsa -in dbt_rsa_key.p8 -pubout -out dbt_rsa_key.pub
-- Paste the public key below without the BEGIN/END lines and line breaks.
-- Keep dbt_rsa_key.p8 private: it goes into dbt Cloud, never into git.
CREATE USER IF NOT EXISTS DBT_USER
  TYPE = SERVICE
  DEFAULT_ROLE = TRANSFORM
  DEFAULT_WAREHOUSE = NSE_WH
  COMMENT = 'Service user for dbt Cloud (key-pair auth)';

GRANT ROLE TRANSFORM TO USER DBT_USER;

ALTER USER DBT_USER SET RSA_PUBLIC_KEY = '<PUBLIC_KEY_BODY>';

-- -----------------------------------------------------------------------------
-- 8. After `terraform apply`: backfill and check
-- -----------------------------------------------------------------------------
-- Load files that landed before the S3 notification existed (last 7 days)
ALTER PIPE NSE_STREAM.RAW.STOCK_CANDLES_PIPE REFRESH;

SELECT SYSTEM$PIPE_STATUS('NSE_STREAM.RAW.STOCK_CANDLES_PIPE');

SELECT COUNT(*)                    AS rows_loaded,
       COUNT(DISTINCT SOURCE_FILE) AS files_loaded,
       MAX(LOADED_AT)              AS last_load
FROM NSE_STREAM.RAW.STOCK_CANDLES_RAW;

-- Optional: show timestamps in IST in Snowsight
-- ALTER USER <YOUR_SNOWFLAKE_USER> SET TIMEZONE = 'Asia/Kolkata';

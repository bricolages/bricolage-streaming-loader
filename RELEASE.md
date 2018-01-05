# Bricolage Streaming Load Release Note

## version 0.13.0

- grand refactoring

## version 0.12.0

- [CHANGE] Adds task_id column to the log table (strload_load_logs).

## version 0.11.0

- Loosen dependent Bricolage version

## version 0.10.2

- [new] New parameter dispatch-interval.

## version 0.10.1

- [fix] Fixes simple variable ref bug.

## version 0.10.0

- [new] Automatically complement strload_jobs status with Redshift-side log table.

## version 0.9.0

- [new] Introduces Redshift-side load log table (strload_load_logs) for load duplication checking.

## version 0.8.1

- [fix] tmp: Do not retry on data connection failure.

## version 0.8.0

- [CHANGE] Loader retries failed load tasks automatically.  Streaming loader does NOT delete a task from the queue on job failures.
- [enhancement] Rewrites loader to get better error handling (ensure to write log record in the wider range of errornous situations).

## version 0.7.1

- fix utilities

## version 0.7.0

- [CHANGE] SQS data source requires "region" attribute.

## version 0.6.2

- [new] AWS access key id and secret key are now optional for SQS data sources (to allow using EC2 instance or ECS task attached IAM roles).
- [new] New utility commands send-data-event, send-shutdown, send-checkpoint, send-load-task.
- Adds sample config files.

## version 0.6.1

- [fix] dispatcher: Default ctl data source was wrong.
- [fix] dispatcher: Detects S3 events by "s3" attribute instead of "eventSource" attribute, to allow fake S3 events (from non-S3 system).
- [fix] dispatcher: SNS alert is now optional.
- [fix] dispatcher: Correctly deletes unknown format messages.
- [enhancement] Adds more logging messages.

## version 0.6.0

- [CHANGE] Adds loaded column to strload_objects table to record if the object is really loaded or not.
- [CHANGE] Now strload_objects' object_url is unique.  Duplicated objects are stored in another table, strload_dup_objects.
- [CHANGE] Now strload_table has table_id column, which is the primary key.
- [new] Loader daemon supports new command line option --working-dir, to support symbolic linked path, such as Capistrano deploy target (current/).
- [new] Keeps Redshift manifest file for later inspection.
- [enhancement] Reduces the number of Redshift writer transactions (1 transaction for 1 loading).
- [enhancement] Delay dispatching tasks until current event batch is processed, to avoid unexpected visibility timeout.
- [enhancement] Adds more logging messages.

## version 0.5.1

- [fix] Fixes slow query

## version 0.5.0

- [new] Introduces FLUSHTABLE dispatcher event

## version 0.4.0

- [new] Introduces CHECKPOINT dispatcher event

## version 0.3.0

- [new] Supoprts SNS notification

## version 0.2.0

- not released
- [fix] Fixes async delete timing

## version 0.1.0

- 2016-07-13 works 1 month

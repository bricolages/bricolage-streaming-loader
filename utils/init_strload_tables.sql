drop table if exists strload_objects cascade;
drop table if exists strload_dup_objects cascade;
drop table if exists strload_task_objects;
drop table if exists strload_tasks cascade;
drop table if exists strload_jobs cascade;

\i schema/strload_objects.ct
\i schema/strload_dup_objects.ct
\i schema/strload_task_objects.ct
\i schema/strload_tasks.ct
\i schema/strload_jobs.ct
\i schema/strload_stats.cv
\i schema/strload_task_status.cv

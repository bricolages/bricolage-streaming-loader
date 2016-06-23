drop table if exists strload_objects cascade;
drop table if exists strload_task_objects;
drop table if exists strload_tasks cascade;
drop table if exists strload_jobs cascade;
drop table if exists strload_datasources cascade;
drop table if exists strload_load_options cascade;

\i schema/strload_objects.ct
\i schema/strload_task_objects.ct
\i schema/strload_tasks.ct
\i schema/strload_jobs.ct
\i schema/strload_datasources.ct
\i schema/strload_load_options.ct
\i schema/strload_stats.cv

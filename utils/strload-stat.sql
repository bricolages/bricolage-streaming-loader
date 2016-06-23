select
    task_seq
    , tbl.source_id
    , tbl.schema_name
    , tbl.table_name
    , submit_time
    , object_count
    , total_object_size
    , job_seq
    , loader_id
    , start_time
    , finish_time
    , status
    , substring(message, 1, 30) as err_msg
from
    strload_tasks t
    inner join (
        select
            task_seq
            , count(*) as object_count
            , sum(object_size) as total_object_size
        from
            strload_task_objects
            inner join strload_objects
                using (object_seq)
        group by 1
        ) o
        using (task_seq)
    left outer join strload_jobs j
        using (task_seq)
    left outer join strload_tables tbl
        using (source_id)
order by
    task_seq
    , job_seq
;

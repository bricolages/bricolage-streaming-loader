require 'bricolage/streamingload/task'
require 'bricolage/streamingload/loaderparams'
require 'bricolage/sqlutils'
require 'json'
require 'securerandom'
require 'forwardable'

module Bricolage

  module StreamingLoad

    class LoadableObject

      extend Forwardable

      def initialize(event, components)
        @event = event
        @components = components
      end

      attr_reader :event

      def_delegator '@event', :url
      def_delegator '@event', :size
      def_delegator '@event', :message_id
      def_delegator '@event', :receipt_handle
      def_delegator '@components', :schema_name
      def_delegator '@components', :table_name

      def data_source_id
        "#{schema_name}.#{table_name}"
      end

      alias qualified_name data_source_id

      def event_time
        @event.time
      end

    end


    class ObjectBuffer

      TASK_GENERATION_TIME_LIMIT = 30 #sec

      include SQLUtils

      def initialize(control_data_source:, logger:)
        @ctl_ds = control_data_source
        @logger = logger
        @task_generation_time_limit = TASK_GENERATION_TIME_LIMIT
      end

      def put(obj)
        @ctl_ds.open {|conn|
          insert_object(conn, obj)
        }
      end

      # Flushes multiple tables periodically
      def flush_tasks
        task_ids = []
        warn_slow_task_generation {
          @ctl_ds.open {|conn|
            conn.transaction {|txn|
              task_ids = insert_tasks(conn)
              insert_task_object_mappings(conn) unless task_ids.empty?
            }
          }
        }
        return task_ids.map {|id| LoadTask.create(task_id: id) }
      end

      # Flushes all objects of all tables immediately with no
      # additional conditions, to create "stream checkpoint".
      def flush_tasks_force
        task_ids  = []
        @ctl_ds.open {|conn|
          conn.transaction {|txn|
            # insert_task_object_mappings may not consume all saved objects
            # (e.g. there are too many objects for one table), we must create
            # tasks repeatedly until there are no unassigned objects.
            until (ids = insert_tasks_force(conn)).empty?
              insert_task_object_mappings(conn)
              task_ids.concat ids
            end
          }
        }
        return task_ids.map {|id| LoadTask.create(task_id: id) }
      end

      # Flushes the all objects of the specified table immediately
      # with no additional conditions, to create "table checkpoint".
      def flush_table_force(table_name)
        task_ids  = []
        @ctl_ds.open {|conn|
          conn.transaction {|txn|
            # insert_task_object_mappings may not consume all saved objects
            # (e.g. there are too many objects for one table), we must create
            # tasks repeatedly until there are no unassigned objects.
            until (ids = insert_table_task_force(conn, table_name)).empty?
              insert_task_object_mappings(conn)
              task_ids.concat ids
            end
          }
        }
        return task_ids.map {|id| LoadTask.create(task_id: id) }
      end

      private

      def insert_object(conn, obj)
        suppress_sql_logging {
          object_ids = conn.query_values(<<-EndSQL)
              insert into strload_objects
                  ( object_url
                  , object_size
                  , data_source_id
                  , message_id
                  , event_time
                  , submit_time
                  )
              values
                  ( #{s obj.url}
                  , #{obj.size}
                  , #{s obj.data_source_id}
                  , #{s obj.message_id}
                  , '#{obj.event_time}' AT TIME ZONE 'JST'
                  , current_timestamp
                  )
              on conflict on constraint strload_objects_object_url
              do nothing
              returning object_id
              ;
          EndSQL
          if object_ids.empty?
            @logger.info "Duplicated object recieved: object_url=#{obj.url}"
            conn.update(<<-EndSQL)
                insert into strload_dup_objects
                    ( object_url
                    , object_size
                    , data_source_id
                    , message_id
                    , event_time
                    , submit_time
                    )
                values
                    ( #{s obj.url}
                    , #{obj.size}
                    , #{s obj.data_source_id}
                    , #{s obj.message_id}
                    , '#{obj.event_time}' AT TIME ZONE 'JST'
                    , current_timestamp
                    )
                ;
            EndSQL
          end
        }
      end

      def insert_tasks_force(conn)
        insert_tasks(conn, force: true)
      end

      def insert_tasks(conn, force: false)
        task_ids = conn.query_values(<<-EndSQL)
            insert into strload_tasks
                ( task_class
                , schema_name
                , table_name
                , submit_time
                )
            select
                'streaming_load_v3'
                , tbl.schema_name
                , tbl.table_name
                , current_timestamp
            from
                strload_tables tbl

                -- number of objects not assigned to a task for each schema_name.table_name (> 0)
                inner join (
                    select
                        data_source_id
                        , count(*) as object_count
                    from
                        (
                            select
                                min(object_id) as object_id
                                , object_url
                            from
                                strload_objects
                            group by
                                object_url
                        ) uniq_objects
                        inner join strload_objects using (object_id)
                        left outer join strload_task_objects using (object_id)
                    where
                        task_id is null -- not assigned to a task
                    group by
                        data_source_id
                ) obj
                using (data_source_id)

                -- preceeding task's submit time
                left outer join (
                    select
                        schema_name
                        , table_name
                        , max(submit_time) as latest_submit_time
                    from
                        strload_tasks
                    group by
                        schema_name, table_name
                ) task
                using (schema_name, table_name)
            where
                not tbl.disabled -- not disabled
                and (
                    #{force ? "true or" : ""}                                                      -- Creates tasks with no conditions if forced
                    obj.object_count > tbl.load_batch_size                                         -- batch_size exceeded?
                    or extract(epoch from current_timestamp - latest_submit_time) > load_interval  -- load_interval exceeded?
                    or latest_submit_time is null                                                  -- no previous tasks?
                )
            returning task_id
            ;
        EndSQL

        log_created_tasks task_ids
        task_ids
      end

      def insert_table_task_force(conn, table_name)
        task_ids = conn.query_values(<<-EndSQL)
            insert into strload_tasks
                ( task_class
                , schema_name
                , table_name
                , submit_time
                )
            select
                'streaming_load_v3'
                , tbl.schema_name
                , tbl.table_name
                , current_timestamp
            from
                strload_tables tbl

                -- The number of objects for each tables, which is not assigned to any task (> 0).
                -- This subquery is covered by the index.
                inner join (
                    select
                        data_source_id
                        , count(*) as object_count
                    from
                        (
                            select
                                min(object_id) as object_id
                                , object_url
                            from
                                strload_objects
                            where
                                data_source_id = #{s table_name}
                            group by
                                object_url
                        ) uniq_objects
                        inner join strload_objects using (object_id)
                        left outer join strload_task_objects using (object_id)
                    where
                        task_id is null -- not assigned to a task
                    group by
                        data_source_id
                ) obj
                using (data_source_id)
            where
                -- does not check disabled
                data_source_id = #{s table_name}
            returning task_id
            ;
        EndSQL

        # It must be 1
        log_created_tasks(task_ids)
        task_ids
      end

      def insert_task_object_mappings(conn)
        conn.update(<<-EndSQL)
            insert into strload_task_objects
                ( task_id
                , object_id
                )
            select
                task_id
                , object_id
            from (
                select
                    row_number() over (partition by task.task_id order by obj.object_id) as object_count
                    , task.task_id
                    , obj.object_id
                    , load_batch_size
                from
                    -- unassigned objects
                    (
                        select
                            data_source_id
                            , uniq_objects.object_url
                            , object_id
                        from
                            (
                                select
                                    min(object_id) as object_id
                                    , object_url
                                from
                                    strload_objects
                                group by
                                    object_url
                            ) uniq_objects
                            inner join strload_objects using(object_id)
                            left outer join strload_task_objects using(object_id)
                        where
                            task_id is null
                     ) obj

                    -- tasks without objects
                    inner join (
                        select
                            tbl.data_source_id
                            , min(task_id) as task_id   -- pick up oldest task
                            , max(load_batch_size) as load_batch_size
                        from
                            strload_tasks
                            inner join strload_tables tbl
                            using (schema_name, table_name)
                        where
                            -- unassigned objects
                            task_id not in (select distinct task_id from strload_task_objects)
                        group by
                            1
                    ) task
                    using (data_source_id)
                ) as t
            where
                object_count <= load_batch_size   -- limit number of objects assigned to single task
            ;
        EndSQL
      end

      def suppress_sql_logging
        # CLUDGE
        orig = @logger.level
        begin
          @logger.level = Logger::ERROR
          yield
        ensure
          @logger.level = orig
        end
      end

      def log_created_tasks(task_ids)
        created_task_num = task_ids.size
        @logger.info "Number of task created: #{created_task_num}"
        @logger.info "Created task ids: #{task_ids}" if created_task_num > 0
      end

      def warn_slow_task_generation(&block)
        start_time = Time.now
        yield
        exec_time = (Time.now - start_time)
        if exec_time > @task_generation_time_limit
          @logger.warn "Long task generation time:  #{exec_time}"
          @task_generation_time_limit = @task_generation_time_limit * 1.1
        end
      end

    end

  end

end

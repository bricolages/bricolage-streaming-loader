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
          suppress_sql_logging {
            conn.transaction {
              object_id = insert_object(conn, obj)
              if object_id
                insert_task_objects(conn, object_id)
              else
                insert_dup_object(conn, obj)
              end
            }
          }
        }
      end

      # Flushes multiple tables periodically
      def flush_tasks
        task_ids = []
        warn_slow_task_generation {
          @ctl_ds.open {|conn|
            conn.transaction {|txn|
              task_ids = insert_tasks(conn)
              unless task_ids.empty?
                update_task_object_mappings(conn, task_ids)
                log_mapped_object_num(conn, task_ids)
              end
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
            # update_task_object_mappings may not consume all saved objects
            # (e.g. there are too many objects for one table), we must create
            # tasks repeatedly until there are no unassigned objects.
            until (ids = insert_tasks_force(conn)).empty?
              update_task_object_mappings(conn, ids)
              log_mapped_object_num(conn, ids)
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
            # update_task_object_mappings may not consume all saved objects
            # (e.g. there are too many objects for one table), we must create
            # tasks repeatedly until there are no unassigned objects.
            until (ids = insert_table_task_force(conn, table_name)).empty?
              update_task_object_mappings(conn, ids)
              log_mapped_object_num(conn, ids)
              task_ids.concat ids
            end
          }
        }
        return task_ids.map {|id| LoadTask.create(task_id: id) }
      end

      private

      def insert_object(conn, obj)
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
        return object_ids.first
      end

      def insert_dup_object(conn, obj)
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

      def insert_task_objects(conn, object_id)
        conn.update(<<-EndSQL)
            insert into strload_task_objects
                ( task_id
                , object_id
                )
            values
                ( -1
                , #{object_id}
                )
            ;
        EndSQL
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
                                object_id
                            from
                                strload_task_objects
                            where
                                task_id = -1
                        ) uniq_objects
                        inner join strload_objects using (object_id)
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
                                object_id
                            from
                                strload_task_objects
                            where
                                task_id = -1
                        ) uniq_objects
                        inner join strload_objects using (object_id)
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

      def update_task_object_mappings(conn, task_ids)
        conn.update(<<-EndSQL)
            update strload_task_objects dst
            set
                task_id = tasks.task_id
            from (
                select
                    object_id
                    , data_source_id
                    , row_number() over(partition by data_source_id order by object_id) as object_number
                from
                    (select object_id from strload_task_objects where task_id = -1) t
                    inner join strload_objects
                        using (object_id)
                    inner join strload_tables
                        using (data_source_id)
                ) tsk_obj
                inner join strload_tables tables
                    using (data_source_id)
                inner join strload_tasks tasks
                    using (schema_name, table_name)
            where
                dst.task_id = -1
                and tasks.task_id in (#{task_ids.join(",")})
                and dst.object_id = tsk_obj.object_id
                and tsk_obj.object_number < tables.load_batch_size
            returning(dst.task_id)
            ;
        EndSQL
      end

      def log_mapped_object_num(conn, task_ids)
        # This method is required since UPDATE does not "returning" multiple values
        rows = conn.query_values(<<-EndSQL)
            select
                task_id
                , count(*)
            from
                strload_task_objects
            where
                task_id in (#{task_ids.join(',')})
            group by
                task_id
            ;
        EndSQL
        rows.each_slice(2) do |row|
          @logger.info "Number of objects assigned to task: task_id=#{row[0]} object_count=#{row[1]}"
        end
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

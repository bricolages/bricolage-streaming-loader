require 'bricolage/streamingload/loadtask'
require 'bricolage/streamingload/chunk'
require 'bricolage/sqlutils'

module Bricolage

  module StreamingLoad

    class ChunkBuffer

      TASK_GENERATION_TIME_LIMIT = 30 #sec

      include SQLUtils

      def initialize(control_data_source:, logger:)
        @ctl_ds = control_data_source
        @logger = logger
        @task_generation_time_limit = TASK_GENERATION_TIME_LIMIT
      end

      # chunk :: IncomingChunk
      def save(chunk)
        @ctl_ds.open {|conn|
          suppress_sql_logging {
            conn.transaction {
              object_id = insert_object(conn, chunk)
              if object_id
                insert_task_objects(conn, object_id)
              else
                @logger.info "Duplicated object recieved: url=#{chunk.url}"
                insert_dup_object(conn, chunk)
              end
            }
          }
        }
      end

      # Flushes chunks of multiple streams, which are met conditions.
      def flush_partial
        task_ids = nil
        tasks = nil

        @ctl_ds.open {|conn|
          warn_slow_task_generation {
            conn.transaction {|txn|
              task_ids = insert_tasks(conn)
              update_task_objects(conn, task_ids) unless task_ids.empty?
            }
          }
          log_task_ids(task_ids)
          tasks = load_tasks(conn, task_ids)
        }
        tasks
      end

      # Flushes all chunks of all stream with no additional conditions,
      # to create "system checkpoint".
      def flush_all
        all_task_ids = []
        tasks = nil

        @ctl_ds.open {|conn|
          conn.transaction {|txn|
            # update_task_objects may not consume all saved objects
            # (e.g. there are too many objects for one table), we must create
            # tasks repeatedly until all objects are flushed.
            until (task_ids = insert_tasks(conn, force: true)).empty?
              update_task_objects(conn, task_ids)
              all_task_ids.concat task_ids
            end
          }
          log_task_ids(all_task_ids)
          tasks = load_tasks(conn, all_task_ids)
        }
        tasks
      end

      # Flushes all chunks of the specified stream with no additional conditions,
      # to create "stream checkpoint".
      def flush_stream(stream_name)
        all_task_ids = []
        tasks = nil

        @ctl_ds.open {|conn|
          conn.transaction {|txn|
            # update_task_objects may not consume all saved objects
            # (e.g. there are too many objects for one table), we must create
            # tasks repeatedly until all objects are flushed.
            until (task_ids = insert_tasks_for_stream(conn, stream_name)).empty?
              update_task_objects(conn, task_ids)
              all_task_ids.concat task_ids
            end
          }
          log_task_ids(all_task_ids)
          tasks = load_tasks(conn, all_task_ids)
        }
        tasks
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
                , #{s obj.stream_name}
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
                , #{s obj.stream_name}
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

      def insert_tasks(conn, force: false)
        task_ids = conn.query_values(<<-EndSQL)
            insert into strload_tasks
                ( task_class
                , table_id
                , submit_time
                )
            select
                'streaming_load_v3'
                , tbl.table_id
                , current_timestamp
            from
                strload_tables tbl

                -- number of objects not assigned to a task for each schema_name.table_name (> 0)
                inner join (
                    select
                        data_source_id
                        , count(*) as object_count
                    from
                        strload_objects
                    where
                        object_id in (select object_id from strload_task_objects where task_id = -1)
                    group by
                        data_source_id
                ) obj
                using (data_source_id)

                -- preceeding task's submit time
                left outer join (
                    select
                        table_id
                        , max(submit_time) as latest_submit_time
                    from
                        strload_tasks
                    group by
                        table_id
                ) task
                using (table_id)
            where
                not tbl.disabled -- not disabled
                and (
                    #{force ? "true or" : ""}                                                      -- Creates tasks with no conditions if forced
                    obj.object_count > tbl.load_batch_size                                         -- batch_size exceeded?
                    or extract(epoch from current_timestamp - task.latest_submit_time) > tbl.load_interval  -- load_interval exceeded?
                    or task.latest_submit_time is null                                             -- no previous tasks?
                )
            returning task_id
            ;
        EndSQL

        task_ids
      end

      def insert_tasks_for_stream(conn, stream_name)
        task_ids = conn.query_values(<<-EndSQL)
            insert into strload_tasks
                ( task_class
                , table_id
                , submit_time
                )
            select
                'streaming_load_v3'
                , tbl.table_id
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
                        strload_objects
                    where
                        object_id in (select object_id from strload_task_objects where task_id = -1)
                    group by
                        data_source_id
                ) obj
                using (data_source_id)
            where
                -- does not check disabled
                data_source_id = #{s stream_name}
            returning task_id
            ;
        EndSQL

        task_ids
      end

      def update_task_objects(conn, task_ids)
        conn.update(<<-EndSQL)
            update strload_task_objects dst
            set
                task_id = tasks.task_id
            from
                strload_tasks tasks
                inner join strload_tables tables using (table_id)
                inner join (
                    select
                        object_id
                        , data_source_id
                        , row_number() over (partition by data_source_id order by object_id) as object_seq
                    from
                        strload_objects
                    where
                        object_id in (select object_id from strload_task_objects where task_id = -1)
                ) tsk_obj
                using (data_source_id)
            where
                dst.task_id = -1
                and tasks.task_id in (#{task_ids.join(",")})
                and dst.object_id = tsk_obj.object_id
                and tsk_obj.object_seq <= tables.load_batch_size
            ;
        EndSQL
        # UPDATE statement cannot return values
        nil
      end

      def load_tasks(conn, task_ids)
        return [] if task_ids.empty?

        records = suppress_sql_logging {
          conn.query_rows(<<-EndSQL)
              select
                  t.task_id
                  , t.object_id
                  , o.object_url
                  , o.object_size
              from
                  strload_task_objects t
                  inner join strload_objects o using (object_id)
              where
                  task_id in (#{task_ids.join(',')})
              ;
          EndSQL
        }

        records.group_by {|row| row['task_id'] }.map {|task_id, rows|
          chunks = rows.map {|row|
            id, url, size = row.values_at('object_id', 'object_url', 'object_size')
            Chunk.new(id: id, url: url, size: size)
          }
          LoadTask.new(id: task_id, chunks: chunks)
        }
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

      def log_task_ids(task_ids)
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

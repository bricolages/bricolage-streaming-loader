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

      include SQLUtils

      def initialize(control_data_source:, logger:)
        @ctl_ds = control_data_source
        @logger = logger
      end

      def put(obj)
        @ctl_ds.open {|conn|
          insert_object(conn, obj)
        }
      end

      def flush_tasks
        task_ids  = []
        @ctl_ds.open {|conn|
          conn.transaction {|txn|
            task_ids = insert_tasks(conn)
            insert_task_object_mappings(conn)
          }
        }
        return task_ids.map {|id| LoadTask.create(task_id: id) }
      end

      private

      def insert_object(conn, obj)
        conn.update(<<-EndSQL)
            insert into strload_objects
                (object_url
                , object_size
                , schema_name
                , table_name
                , message_id
                , event_time
                , submit_time
                )
            select
                #{s obj.url}
                , #{obj.size}
                , schema_name
                , table_name
                , #{s obj.message_id}
                , '#{obj.event_time}' AT TIME ZONE 'JST'
                , current_timestamp
            from
                strload_tables
            where
                data_source_id = #{s obj.data_source_id}
                and not exists (select * from strload_objects where object_url = #{s obj.url})
            ;
        EndSQL
      end

      def insert_tasks(conn)
        vals = conn.query_values(<<-EndSQL)
          insert into
              strload_tasks (task_class, schema_name, table_name, submit_time)
          select
              'streaming_load_v3'
              , tbl.schema_name
              , tbl.table_name
              , current_timestamp
          from
              strload_tables tbl
              inner join (
                  select
                      schema_name
                      , table_name
                      , count(*) as object_count
                  from
                      strload_objects t1
                      left outer join strload_task_objects
                          using(object_id)
                  where
                      task_id is null -- not assigned to a task
                  group by
                      schema_name, table_name
                  ) obj -- number of objects not assigned to a task (won't return zero)
                  using (schema_name, table_name)
              left outer join (
                  select
                      schema_name
                      , table_name
                      , max(submit_time) as latest_submit_time
                  from
                      strload_tasks
                  group by
                      schema_name, table_name
                  ) task -- preceeding task's submit time
                  using(schema_name, table_name)
          where
              not tbl.disabled -- not disabled
              and (
                obj.object_count > tbl.load_batch_size -- batch_size exceeded?
                or extract(epoch from current_timestamp - latest_submit_time) > load_interval -- load_interval exceeded?
                or latest_submit_time is null -- no last task
              )
          returning task_id
          ;
        EndSQL
        @logger.info "Number of task created: #{vals.size}"
        vals
      end

      def insert_task_object_mappings(conn)
        conn.update(<<-EndSQL)
          insert into
              strload_task_objects
          select
              task_id
              , object_id
          from (
              select
                  row_number() over(partition by task.task_id order by obj.object_id) as object_count
                  , task.task_id
                  , obj.object_id
                  , load_batch_size
              from
                  strload_objects obj
                  inner join (
                      select
                          min(task_id) as task_id -- oldest task
                          , tbl.schema_name
                          , tbl.table_name
                          , max(load_batch_size) as load_batch_size
                      from
                          strload_tasks
                          inner join strload_tables tbl
                              using(schema_name, table_name)
                      where
                          task_id not in (select distinct task_id from strload_task_objects) -- no assigned objects
                      group by
                          2
                          , 3
                      ) task -- tasks without objects
                      using(schema_name, table_name)
                  left outer join strload_task_objects task_obj
                      using(object_id)
              where
                  task_obj.object_id is null -- not assigned to a task
              ) as t
          where
              object_count <= load_batch_size -- limit number of objects assigned to single task
          ;
        EndSQL
      end

    end

  end

end

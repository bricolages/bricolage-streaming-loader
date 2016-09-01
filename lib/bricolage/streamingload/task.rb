require 'bricolage/sqsdatasource'
require 'json'

module Bricolage

  module StreamingLoad

    class Task < SQSMessage

      def Task.get_concrete_class(msg, rec)
        case
        when rec['eventName'] == 'streaming_load_v3' then LoadTask
        else
          raise "[FATAL] unknown SQS message record: eventSource=#{rec['eventSource']} event=#{rec['eventName']} message_id=#{msg.message_id}"
        end
      end

      def message_type
        raise "#{self.class}\#message_type must be implemented"
      end

      def data?
        false
      end

    end


    class LoadTask < Task

      def LoadTask.create(task_id:, force: false)
        super name: 'streaming_load_v3', task_id: task_id, force: force
      end

      def LoadTask.parse_sqs_record(msg, rec)
        {
          task_id: rec['taskId'],
          force: (rec['force'].to_s == 'true')
        }
      end

      def LoadTask.load(conn, task_id, force: false)
        rec = conn.query_row(<<-EndSQL)
          select
              task_class
              , tbl.schema_name
              , tbl.table_name
              , disabled
          from
              strload_tasks tsk
              inner join strload_tables tbl
                  using(schema_name, table_name)
          where
              task_id = #{task_id}
          ;
        EndSQL
        object_urls = conn.query_values(<<-EndSQL)
          select
              object_url
          from
              strload_task_objects
              inner join strload_objects
              using (object_id)
              inner join strload_tasks
              using (task_id)
          where
              task_id = #{task_id}
          ;
        EndSQL
        return nil unless rec
        new(
          name: rec['task_class'],
          time: nil,
          source: nil,
          task_id: task_id,
          schema: rec['schema_name'],
          table: rec['table_name'],
          object_urls: object_urls,
          disabled: rec['disabled'] == 'f' ? false : true,
          force: force
        )
      end

      alias message_type name

      def init_message(task_id:, schema: nil, table: nil, object_urls: nil, disabled: false, force: false)
        @id = task_id
        @force = force

        # Effective only for queue reader process
        @schema = schema
        @table = table
        @object_urls = object_urls
        @disabled = disabled
      end

      attr_reader :id

      def force?
        !!@force
      end

      #
      # For writer only
      #

      attr_reader :schema, :table, :object_urls, :disabled

      def qualified_name
        "#{@schema}.#{@table}"
      end

      def body
        obj = super
        obj['taskId'] = @id
        obj['schemaName'] = @schema
        obj['tableName'] = @table
        obj['objectUrls'] = @object_urls
        obj['disabled'] = @disabled
        obj['force'] = @force
        obj
      end

    end

  end   # module StreamingLoad

end   # module Bricolage

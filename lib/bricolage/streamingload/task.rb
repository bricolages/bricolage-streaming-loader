require 'bricolage/sqsdatasource'

module Bricolage

  module StreamingLoad

    class Task < SQSMessage

      def Task.get_concrete_class(msg, rec)
        case
        when rec['eventName'] == 'streaming_load_v3' then LoadTask
        else UnknownSQSMessage
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

      alias message_type name

      def init_message(task_id:, force: false)
        @task_id = task_id
        @force = force
      end

      attr_reader :task_id

      def force?
        !!@force
      end

      def body
        obj = super
        obj['taskId'] = @task_id
        obj['force'] = true if @force
        obj
      end

    end

  end   # module StreamingLoad

end   # module Bricolage

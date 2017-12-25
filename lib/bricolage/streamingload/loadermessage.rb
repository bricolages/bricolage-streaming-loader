require 'bricolage/sqsdatasource'

module Bricolage

  module StreamingLoad

    class LoaderMessage < SQSMessage

      def LoaderMessage.get_concrete_class(msg, rec)
        case
        when rec['eventName'] == 'streaming_load_v3' then StreamingLoadV3LoaderMessage
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


    class StreamingLoadV3LoaderMessage < LoaderMessage

      def StreamingLoadV3LoaderMessage.create(task_id:, force: false)
        super name: 'streaming_load_v3', task_id: task_id, force: force
      end

      def StreamingLoadV3LoaderMessage.parse_sqs_record(msg, rec)
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

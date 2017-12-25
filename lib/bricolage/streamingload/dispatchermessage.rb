require 'bricolage/sqsdatasource'

module Bricolage

  module StreamingLoad

    class DispatcherMessage < SQSMessage

      def DispatcherMessage.get_concrete_class(msg, rec)
        case
        when rec['eventName'] == 'shutdown' then ShutdownDispatcherMessage
        when rec['eventName'] == 'dispatch' then DispatchDispatcherMessage
        when rec['eventName'] == 'flushtable' then FlushTableDispatcherMessage
        when rec['eventName'] == 'checkpoint' then CheckPointDispatcherMessage
        when !!rec['s3'] then S3ObjectDispatcherMessage
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


    class ShutdownDispatcherMessage < DispatcherMessage

      def ShutdownDispatcherMessage.create
        super name: 'shutdown'
      end

      def ShutdownDispatcherMessage.parse_sqs_record(msg, rec)
        {}
      end

      alias message_type name

      def init_message(dummy: nil)
      end

    end


    # Flushes all tables and shutdown
    class CheckPointDispatcherMessage < DispatcherMessage

      def CheckPointDispatcherMessage.create
        super name: 'checkpoint'
      end

      def CheckPointDispatcherMessage.parse_sqs_record(msg, rec)
        {}
      end

      alias message_type name

      def init_message(dummy: nil)
      end

    end


    class FlushTableDispatcherMessage < DispatcherMessage

      def FlushTableDispatcherMessage.create(table_name:)
        super name: 'flushtable', table_name: table_name
      end

      def FlushTableDispatcherMessage.parse_sqs_record(msg, rec)
        {
          table_name: rec['tableName']
        }
      end

      alias message_type name

      def init_message(table_name:)
        @table_name = table_name
      end

      attr_reader :table_name

      def body
        obj = super
        obj['tableName'] = @table_name
        obj
      end

    end


    class DispatchDispatcherMessage < DispatcherMessage

      def DispatchDispatcherMessage.create(delay_seconds:)
        super name: 'dispatch', delay_seconds: delay_seconds
      end

      alias message_type name

      def init_message(dummy: nil)
      end

    end


    class S3ObjectDispatcherMessage < DispatcherMessage

      def S3ObjectDispatcherMessage.parse_sqs_record(msg, rec)
        {
          region: rec['awsRegion'],
          bucket: rec['s3']['bucket']['name'],
          key: rec['s3']['object']['key'],
          size: rec['s3']['object']['size']
        }
      end

      def message_type
        'data'
      end

      def init_message(region:, bucket:, key:, size:)
        @region = region
        @bucket = bucket
        @key = key
        @size = size
      end

      attr_reader :region
      attr_reader :bucket
      attr_reader :key
      attr_reader :size

      def url
        "s3://#{@bucket}/#{@key}"
      end

      # override
      def data?
        true
      end

      def created?
        !!(/\AObjectCreated:(?!Copy)/ =~ @name)
      end

      def loadable_object(url_patterns)
        LoadableObject.new(self, url_patterns.match(url))
      end

    end

  end

end

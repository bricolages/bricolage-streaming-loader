require 'bricolage/sqsdatasource'
require 'json'

module Bricolage

  def SQSDataSource.new_mock(**args)
    SQSDataSource.new(
      region: 'ap-northeast-1',
      url: 'http://sqs/000000000000/queue-name',
      access_key_id: 'access_key_id_1',
      secret_access_key: 'secret_access_key_1',
      visibility_timeout: 30
    ).tap {|ds|
      logger = NullLogger.new
      #logger = Bricolage::Logger.default
      ds.__send__(:initialize_base, 'name', nil, logger)
      ds.instance_variable_set(:@client, SQSMock::Client.new(**args))
    }
  end

  module SQSMock

    class Client
      def initialize(queue: [], receive_message: nil, send_message: nil, delete_message: nil, delete_message_batch: nil)
        @queue = queue   # [[record]]
        @call_history = []

        @receive_message = receive_message || lambda {|**args|
          msgs = @queue.shift or break ReceiveMessageResponse.successful([])
          ReceiveMessageResponse.successful(msgs)
        }

        @send_message = send_message || lambda {|**args|
          SendMessageResponse.successful
        }

        @delete_message = delete_message || lambda {|**args|
          Response.successful
        }

        @delete_message_batch = delete_message_batch || lambda {|queue_url:, entries:|
          # Returns success for all requests by default.
          DeleteMessageBatchResponse.new.tap {|res|
            entries.each do |ent|
              res.add_success_for(ent)
            end
          }
        }
      end

      # Free free to modify this array contents
      attr_reader :call_history

      def self.def_mock_method(name)
        define_method(name) {|**args|
          @call_history.push CallHistory.new(name.intern, args)
          instance_variable_get("@#{name}").(**args)
        }
      end

      def_mock_method :receive_message
      def_mock_method :send_message
      def_mock_method :delete_message
      def_mock_method :delete_message_batch
    end

    CallHistory = Struct.new(:name, :args)

    # success/failure only result
    class Response
      def Response.successful
        new(successful: true)
      end

      def initialize(successful:)
        @successful = successful
      end

      def successful?
        @successful
      end
    end

    class ReceiveMessageResponse < Response
      def ReceiveMessageResponse.successful(msgs)
        new(successful: true, messages: msgs)
      end

      def initialize(successful:, messages:)
        super(successful: successful)
        @messages = messages
      end

      attr_reader :messages
    end

    class SendMessageResponse < Response
      def SendMessageResponse.successful
        new(successful: true, message_id: "sqs-sent-message-id-#{Message.new_seq}")
      end

      def initialize(successful:, message_id:)
        super(successful: successful)
        @message_id = message_id
      end

      attr_reader :message_id
    end

    class DeleteMessageBatchResponse
      def initialize(successful: [], failed: [])
        @successful = successful
        @failed = failed
      end

      attr_reader :successful
      attr_reader :failed

      Success = Struct.new(:id)
      Failure = Struct.new(:id, :sender_fault, :code, :message)

      def add_success_for(ent)
        @successful.push Success.new(ent[:id])
      end

      def add_failure_for(ent)
        @failed.push Failure.new(ent[:id], true, '400', 'some reason')
      end
    end

    class Message
      def Message.s3_object_created_event(url)
        raise "is not a S3 URL: #{url.inspect}" unless %r<\As3://\w> =~ url
        bucket, key = url.sub(%r<s3://>, '').split('/', 2)
        with_body({
          eventVersion: '2.0',
          eventSource: 'aws:s3',
          awsRegion: 'ap-northeast-1',
          eventTime: Time.now.iso8601,
          eventName: 'ObjectCreated:Put',
          s3: {
            s3SchemaVersion: '1.0',
            configurationId: 'TestConfig',
            bucket: {
              name: bucket,
              arn: "arn:aws:s3:::#{bucket}"
            },
            object: {
              key: key,
              size: 1024
            }
          }
        })
      end

      @seq = 0

      def Message.new_seq
        @seq += 1
        @seq
      end

      def Message.with_body(body)
        seq = new_seq
        new(
          message_id: "sqs-message-id-#{seq}",
          receipt_handle: "sqs-receipt-handle-#{seq}",
          body: body
        )
      end

      def initialize(message_id: nil, receipt_handle: nil, body: nil)
        @message_id = message_id
        @receipt_handle = receipt_handle
        @body = body
        @body_json = { Records: [body] }.to_json
      end

      attr_reader :message_id
      attr_reader :receipt_handle

      def body
        @body_json
      end

      # for debug
      def body_object
        @body
      end
    end

  end   # module SQSMock

end   # module Bricolage

require 'test/unit'
require 'bricolage/streamingload/event'
require 'bricolage/logger'

module Bricolage

  class TestSQSDataSource < Test::Unit::TestCase

    def new_sqs_ds(mock_client = nil)
      SQSDataSource.new(
        url: 'http://sqs/000000000000/queue-name',
        access_key_id: 'access_key_id_1',
        secret_access_key: 'secret_access_key_1',
        visibility_timeout: 30
      ).tap {|ds|
        logger = NullLogger.new
        #logger = Bricolage::Logger.default
        ds.__send__(:initialize_base, 'name', nil, logger)
        ds.instance_variable_set(:@client, mock_client) if mock_client
      }
    end

    class MockSQSClient
      def initialize(&block)
        @handler = block
      end

      def delete_message_batch(**args)
        @handler.call(args)
      end
    end

    class NullLogger
      def debug(*args) end
      def info(*args) end
      def warn(*args) end
      def error(*args) end
      def exception(*args) end
      def with_elapsed_time(*args) yield end
      def elapsed_time(*args) yield end
    end

    def sqs_message(seq)
      MockSQSMessage.new("message_id_#{seq}", "receipt_handle_#{seq}")
    end

    MockSQSMessage = Struct.new(:message_id, :receipt_handle)

    class MockSQSResponse
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

    test "#delete_message_async" do
      messages = [sqs_message(0), sqs_message(1), sqs_message(2)]
      mock = MockSQSClient.new {|args|
        entries = args[:entries]
        if entries.size == 3
          # first time
          assert_equal messages[0].receipt_handle, entries[0][:receipt_handle]
          assert_equal messages[1].receipt_handle, entries[1][:receipt_handle]
          assert_equal messages[2].receipt_handle, entries[2][:receipt_handle]
          MockSQSResponse.new.tap {|res|
            res.add_success_for(entries[0])
            res.add_failure_for(entries[1])
            res.add_success_for(entries[2])
          }
        else
          # second time
          MockSQSResponse.new.tap {|res|
            res.add_success_for(entries[0])
          }
        end
      }
      ds = new_sqs_ds(mock)
      ds.delete_message_async(messages[0])
      ds.delete_message_async(messages[1])
      ds.delete_message_async(messages[2])

      # first flush
      flush_time = Time.now
      ds.delete_message_buffer.flush(flush_time)
      assert_equal 1, ds.delete_message_buffer.size
      bufent = ds.delete_message_buffer.instance_variable_get(:@buf).values.first
      assert_equal 'receipt_handle_1', bufent.message.receipt_handle
      assert_equal 1, bufent.n_failure
      assert_false bufent.issuable?(flush_time)
      assert_true bufent.issuable?(flush_time + 180)

      # second flush
      ds.delete_message_buffer.flush(flush_time + 180)
      assert_true ds.delete_message_buffer.empty?
    end

  end

end

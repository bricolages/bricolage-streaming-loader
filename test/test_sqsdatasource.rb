require 'test/unit'
require 'bricolage/streamingload/event'
require 'bricolage/sqsmock'
require 'bricolage/logger'

module Bricolage

  class TestSQSDataSource < Test::Unit::TestCase

    test "#delete_message_async" do
      messages = (0..2).map {|seq|
        SQSMock::Message.new(message_id: "message_id_#{seq}", receipt_handle: "receipt_handle_#{seq}")
      }
      ds = SQSDataSource.new_mock(
        delete_message_batch: -> (queue_url:, entries:) {
          if entries.size == 3
            # first time
            assert_equal messages[0].receipt_handle, entries[0][:receipt_handle]
            assert_equal messages[1].receipt_handle, entries[1][:receipt_handle]
            assert_equal messages[2].receipt_handle, entries[2][:receipt_handle]
            SQSMock::DeleteMessageBatchResponse.new.tap {|res|
              res.add_success_for(entries[0])
              res.add_failure_for(entries[1])
              res.add_success_for(entries[2])
            }
          else
            # second time
            SQSMock::DeleteMessageBatchResponse.new.tap {|res|
              res.add_success_for(entries[0])
            }
          end
        }
      )

      ds.delete_message_async(messages[0])
      ds.delete_message_async(messages[1])
      ds.delete_message_async(messages[2])

      # first flush
      flush_time = Time.now
      ds.process_async_delete(flush_time)
      delete_buf = ds.__send__(:delete_message_buffer)
      bufent = delete_buf.instance_variable_get(:@buf).values.first
      assert_equal 'receipt_handle_1', bufent.message.receipt_handle
      assert_equal 1, bufent.n_failure
      assert_false bufent.issuable?(flush_time)
      assert_true bufent.issuable?(flush_time + 180)

      # second flush
      ds.process_async_delete(flush_time + 180)
      assert_true delete_buf.empty?
    end

  end

end

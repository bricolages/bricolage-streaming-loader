require 'test/unit'
require 'bricolage/context'
require 'bricolage/sqsdatasource'
require 'bricolage/sqsmock'
require 'bricolage/streamingload/dispatcher'

module Bricolage
  module StreamingLoad

    class TestDispatcher < Test::Unit::TestCase

      test "checkpoint event" do
        ctx = Context.for_application('.', environment: 'test', logger: NullLogger.new)
        ctl_ds = ctx.get_data_source('sql', 'dwhctl')

        event_queue = SQSDataSource.new_mock(queue: [
          # 1st ReceiveMessage
          [
            SQSMock::Message.s3_object_created_event('s3://test-bucket/testschema.desttable/datafile-0001.json.gz'),
            SQSMock::Message.s3_object_created_event('s3://test-bucket/testschema.desttable/datafile-0002.json.gz'),
            SQSMock::Message.s3_object_created_event('s3://test-bucket/testschema.desttable/datafile-0003.json.gz'),
            SQSMock::Message.s3_object_created_event('s3://test-bucket/testschema.desttable/datafile-0004.json.gz'),
            SQSMock::Message.s3_object_created_event('s3://test-bucket/testschema.desttable/datafile-0005.json.gz')
          ],
          # 2nd ReceiveMessage
          [
            SQSMock::Message.s3_object_created_event('s3://test-bucket/testschema.desttable/datafile-0006.json.gz'),
            SQSMock::Message.s3_object_created_event('s3://test-bucket/testschema.desttable/datafile-0007.json.gz'),
            SQSMock::Message.s3_object_created_event('s3://test-bucket/testschema.desttable/datafile-0008.json.gz'),
            SQSMock::Message.s3_object_created_event('s3://test-bucket/testschema.desttable/datafile-0009.json.gz'),
            SQSMock::Message.new(body: {eventSource: 'bricolage:system', eventName: 'checkpoint'}),
            SQSMock::Message.s3_object_created_event('s3://test-bucket/testschema.desttable/datafile-0010.json.gz')
          ]
        ])

        task_queue = SQSDataSource.new_mock

        object_buffer = ObjectBuffer.new(
          control_data_source: ctl_ds,
          logger: ctx.logger
        )

        url_patterns = URLPatterns.for_config([
          {
            "url" => %r<\As3://test-bucket/testschema\.desttable/datafile-\d{4}\.json\.gz>.source,
            "schema" => 'testschema',
            "table" => 'desttable'
          }
        ])

        dispatcher = Dispatcher.new(
          event_queue: event_queue,
          task_queue: task_queue,
          object_buffer: object_buffer,
          url_patterns: url_patterns,
          dispatch_interval: 600,
          logger: ctx.logger
        )

        # FIXME: database cleaner
        ctl_ds.open {|conn|
          conn.update("truncate strload_tables")
          conn.update("truncate strload_objects")
          conn.update("truncate strload_task_objects")
          conn.update("truncate strload_tasks")
          conn.update("insert into strload_tables values ('testschema', 'desttable', 'testschema.desttable', 100, 1800, false)")
        }
        dispatcher.event_loop

        # Event Queue Call Sequence
        hst = event_queue.client.call_history
        assert_equal :send_message, hst[0].name      # start flush timer
        assert_equal :receive_message, hst[1].name
        assert_equal :delete_message_batch, hst[2].name
        assert_equal :receive_message, hst[3].name
        assert_equal :delete_message, hst[4].name    # delete checkpoint
        assert_equal :delete_message_batch, hst[5].name

        # Task Queue Call Sequence
        hst = task_queue.client.call_history
        assert_equal :send_message, hst[0].name
        assert(/streaming_load_v3/ =~ hst[0].args[:message_body])
        task_id = JSON.load(hst[0].args[:message_body])['Records'][0]['taskId'].to_i
        assert_not_equal 0, task_id

        # Object Buffer
        assert_equal [], unassigned_objects(ctl_ds)
        task = ctl_ds.open {|conn| LoadTask.load(conn, task_id) }
        assert_equal 'testschema', task.schema
        assert_equal 'desttable', task.table
        assert_equal 10, task.object_urls.size
      end

      def unassigned_objects(ctl_ds)
        ctl_ds.open {|conn|
          conn.query_values(<<-EndSQL)
              select
                  object_url
              from
                  strload_objects
              where
                  object_id not in (select object_id from strload_task_objects)
              ;
          EndSQL
        }
      end

    end

  end
end

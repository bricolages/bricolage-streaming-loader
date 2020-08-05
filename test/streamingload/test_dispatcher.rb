require 'test/unit'
require 'bricolage/context'
require 'bricolage/sqsdatasource'
require 'bricolage/sqsmock'
require 'bricolage/streamingload/dispatcher'
require 'bricolage/streamingload/chunkrouter'
require 'bricolage/streamingload/chunkbuffer'

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

        chunk_buffer = ChunkBuffer.new(
          control_data_source: ctl_ds,
          logger: ctx.logger
        )

        chunk_router = ChunkRouter.for_config([
          {
            "url" => %r<\As3://test-bucket/testschema\.desttable/datafile-\d{4}\.json\.gz>.source,
            "schema" => 'testschema',
            "table" => 'desttable'
          }
        ])

        task_logger = DummyLoadTaskLogger.new

        dispatcher = Dispatcher.new(
          event_queue: event_queue,
          task_queue: task_queue,
          chunk_buffer: chunk_buffer,
          chunk_router: chunk_router,
          task_logger: task_logger,
          dispatch_interval: 600,
          logger: ctx.logger
        )

        # FIXME: database cleaner
        ctl_ds.open {|conn|
          conn.update("truncate strload_tables")
          conn.update("truncate strload_objects")
          conn.update("truncate strload_task_objects")
          conn.update("truncate strload_tasks")
          conn.update("insert into strload_tables values (1, 'testschema.desttable', 'testschema', 'desttable', 100, 1800, false, 'test-bucket', 'test-prefix')")
        }
        dispatcher.event_loop

        # Event Queue Call Sequence
        hst = event_queue.client.call_history
        assert_equal 6, hst.size
        assert_equal :send_message, hst[0].name      # start flush timer
        assert_equal :receive_message, hst[1].name
        assert_equal :delete_message_batch, hst[2].name
        assert_equal :receive_message, hst[3].name
        assert_equal :delete_message, hst[4].name    # delete checkpoint
        assert_equal :delete_message_batch, hst[5].name

        # Task Queue Call Sequence
        hst = task_queue.client.call_history
        assert_equal 1, hst.size
        assert_equal :send_message, hst[0].name
        assert(/streaming_load_v3/ =~ hst[0].args[:message_body])
        task_id = JSON.load(hst[0].args[:message_body])['Records'][0]['taskId'].to_i
        assert_not_equal 0, task_id

        # Object Buffer
        assert_equal [], unassigned_objects(ctl_ds)
        task = Job::ControlConnection.open(ctl_ds) {|ctl| ctl.load_task(task_id) }
        assert_equal 'testschema', task.schema_name
        assert_equal 'desttable', task.table_name
        assert_equal 10, task.object_urls.size

        # Task Logger
        assert_not_nil task_logger.last_task
        assert_equal task_id, task_logger.last_task.id.to_i
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

      test "flushtable event" do
        ctx = Context.for_application('.', environment: 'test', logger: NullLogger.new)
        ctl_ds = ctx.get_data_source('sql', 'dwhctl')

        event_queue = SQSDataSource.new_mock(queue: [
          # 1st ReceiveMessage
          [
            SQSMock::Message.s3_object_created_event('s3://test-bucket/testschema.aaa/datafile-0001.json.gz'),
            SQSMock::Message.s3_object_created_event('s3://test-bucket/testschema.bbb/datafile-0001.json.gz'),
            SQSMock::Message.s3_object_created_event('s3://test-bucket/testschema.ccc/datafile-0002.json.gz'),
            SQSMock::Message.s3_object_created_event('s3://test-bucket/testschema.aaa/datafile-0002.json.gz'),
            SQSMock::Message.s3_object_created_event('s3://test-bucket/testschema.bbb/datafile-0003.json.gz'),
            SQSMock::Message.s3_object_created_event('s3://test-bucket/testschema.ccc/datafile-0003.json.gz'),
            SQSMock::Message.new(body: {eventSource: 'bricolage:system', eventName: 'flushtable', tableName: 'testschema.bbb'}),
            SQSMock::Message.new(body: {eventSource: 'bricolage:system', eventName: 'shutdown'})
          ]
        ])

        task_queue = SQSDataSource.new_mock

        chunk_buffer = ChunkBuffer.new(
          control_data_source: ctl_ds,
          logger: ctx.logger
        )

        chunk_router = ChunkRouter.for_config([
          {
            "url" => %r<\As3://test-bucket/testschema\.(?<table>\w+)/datafile-\d{4}\.json\.gz>.source,
            "schema" => 'testschema',
            "table" => '%table'
          }
        ])

        task_logger = DummyLoadTaskLogger.new

        dispatcher = Dispatcher.new(
          event_queue: event_queue,
          task_queue: task_queue,
          chunk_buffer: chunk_buffer,
          chunk_router: chunk_router,
          task_logger: task_logger,
          dispatch_interval: 600,
          logger: ctx.logger
        )

        # FIXME: database cleaner
        ctl_ds.open {|conn|
          conn.update("truncate strload_tables")
          conn.update("truncate strload_objects")
          conn.update("truncate strload_task_objects")
          conn.update("truncate strload_tasks")
          conn.update("insert into strload_tables values (1, 'testschema.aaa', 'testschema', 'aaa', 100, 1800, false, 'test-bucket', 'test-prefix')")
          conn.update("insert into strload_tables values (2, 'testschema.bbb', 'testschema', 'bbb', 100, 1800, false, 'test-bucket', 'test-prefix')")
          conn.update("insert into strload_tables values (3, 'testschema.ccc', 'testschema', 'ccc', 100, 1800, false, 'test-bucket', 'test-prefix')")
        }
        dispatcher.event_loop

        # Event Queue Call Sequence
        hst = event_queue.client.call_history
        assert_equal 5, hst.size
        assert_equal :send_message, hst[0].name      # start dispatch timer
        assert_equal :receive_message, hst[1].name
        assert_equal :delete_message, hst[2].name    # delete flushtable event
        assert_equal :delete_message, hst[3].name    # delete shutdown event
        assert_equal :delete_message_batch, hst[4].name

        # Task Queue Call Sequence
        hst = task_queue.client.call_history
        assert_equal 1, hst.size
        assert_equal :send_message, hst[0].name
        assert(/streaming_load_v3/ =~ hst[0].args[:message_body])
        task_id = JSON.load(hst[0].args[:message_body])['Records'][0]['taskId'].to_i
        assert_not_equal 0, task_id

        # Object Buffer
        assert_equal [], unassigned_table_objects(ctl_ds, 'testschema.bbb')
        task = Job::ControlConnection.open(ctl_ds) {|ctl| ctl.load_task(task_id) }
        assert_equal 'testschema', task.schema_name
        assert_equal 'bbb', task.table_name
        assert_equal 2, task.object_urls.size

        # Task Logger
        assert_not_nil task_logger.last_task
        assert_equal task_id, task_logger.last_task.id.to_i
      end

      def unassigned_table_objects(ctl_ds, table_name)
        ctl_ds.open {|conn|
          conn.query_values(<<-EndSQL)
              select
                  object_url
              from
                  strload_objects
              where
                  data_source_id = '#{table_name}'
                  and object_id not in (select object_id from strload_task_objects)
              ;
          EndSQL
        }
      end

    end


    class DummyLoadTaskLogger

      def initialize
        @task = nil
      end

      def log(task)
        @task = task
        nil
      end

      def last_task
        @task
      end

    end

  end
end

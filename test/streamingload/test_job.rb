require 'test/unit'
require 'bricolage/streamingload/job'
require 'bricolage/context'
require 'bricolage/logger'

module Bricolage

  # FIXME: patch
  class NullLogger
    def log(*args) end
    def add(*args) end
  end

  module StreamingLoad

    class TestJob < Test::Unit::TestCase

      test "execute_task" do
        setup_context {|ctx, ctl_ds, db|
          db.insert_into 'strload_tables', [1, 'testschema.desttable', 'testschema', 'desttable', 100, 1800, false]
          db.insert_into 'strload_tasks', [1, 'streaming_load_v3', 1, current_timestamp]
          db.insert_into 'strload_task_objects', [1, 1], [1, 2]
          db.insert_into 'strload_objects',
            [1, 's3://data-bucket/testschema.desttable/0001.json.gz', 1024, 'testschema.desttable', 'mmmm', current_timestamp, current_timestamp],
            [2, 's3://data-bucket/testschema.desttable/0002.json.gz', 1024, 'testschema.desttable', 'mmmm', current_timestamp, current_timestamp]

          job = Job.new(context: ctx, ctl_ds: ctl_ds, task_id: 1, force: false, logger: ctx.logger)
          job.execute_task

          copy_stmt = "copy testschema.desttable from '#{job.manifest.url}' credentials 'cccc' manifest statupdate false compupdate false json 'auto' gzip timeformat 'auto' dateformat 'auto' acceptanydate acceptinvchars ' ' truncatecolumns trimblanks ;"
          assert_equal [copy_stmt], job.data_ds.sql_list
          job_row = db.query_row("select * from strload_jobs where job_id = #{job.job_id}")
          assert_equal 1, job_row['task_id'].to_i
          assert_equal job.process_id, job_row['process_id']
          assert_equal 'success', job_row['status']
        }
      end

      test "execute_task (with work table)" do
        setup_context {|ctx, ctl_ds, db|
          db.insert_into 'strload_tables', [1, 'testschema.with_work_table', 'testschema', 'with_work_table', 100, 1800, false]
          db.insert_into 'strload_tasks', [11, 'streaming_load_v3', 1, current_timestamp]
          db.insert_into 'strload_task_objects', [11, 1001], [11, 1002]
          db.insert_into 'strload_objects',
            [1001, 's3://data-bucket/testschema.with_work_table/0001.json.gz', 1024, 'testschema.with_work_table', 'mmmm', current_timestamp, current_timestamp],
            [1002, 's3://data-bucket/testschema.with_work_table/0002.json.gz', 1024, 'testschema.with_work_table', 'mmmm', current_timestamp, current_timestamp]

          job = Job.new(context: ctx, ctl_ds: ctl_ds, task_id: 11, force: false, logger: ctx.logger)
          job.execute_task

          assert_equal 'begin transaction;', job.data_ds.sql_list[0]
          assert_equal 'delete from testschema.with_work_table_wk', job.data_ds.sql_list[1]
          assert_equal "copy testschema.with_work_table_wk from '#{job.manifest.url}' credentials 'cccc' manifest statupdate false compupdate false json 'auto' gzip timeformat 'auto' dateformat 'auto' acceptanydate acceptinvchars ' ' truncatecolumns trimblanks ;", job.data_ds.sql_list[2]
          assert_equal "insert into testschema.with_work_table select * from testschema.with_work_table_wk;\n", job.data_ds.sql_list[3]
          assert_equal 'truncate testschema.with_work_table_wk;', job.data_ds.sql_list[4]

          job_row = db.query_row("select * from strload_jobs where job_id = #{job.job_id}")
          assert_equal 11, job_row['task_id'].to_i
          assert_equal job.process_id, job_row['process_id']
          assert_equal 'success', job_row['status']
        }
      end

      test "execute_task (disabled)" do
        setup_context {|ctx, ctl_ds, db|
          db.insert_into 'strload_tables', [1, 'testschema.desttable', 'testschema', 'desttable', 100, 1800, true]
          db.insert_into 'strload_tasks', [1, 'streaming_load_v3', 1, current_timestamp]

          job = Job.new(context: ctx, ctl_ds: ctl_ds, task_id: 1, force: false, logger: ctx.logger)
          assert_raise(JobDefered) {
            job.execute_task
          }
          count = db.query_value("select count(*) from strload_jobs")
          assert_equal 0, count.to_i
        }
      end

      test "execute_task (duplicated)" do
        setup_context {|ctx, ctl_ds, db|
          db.insert_into 'strload_tables', [1, 'testschema.desttable', 'testschema', 'desttable', 100, 1800, false]
          db.insert_into 'strload_tasks', [1, 'streaming_load_v3', 1, current_timestamp]
          db.insert_into 'strload_jobs',
            [1, 1, 'localhost-1234', 'failure', current_timestamp, current_timestamp, ''],
            [2, 1, 'localhost-1234', 'success', current_timestamp, current_timestamp, ''],
            [3, 1, 'localhost-1234', 'duplicated', current_timestamp, current_timestamp, '']

          job = Job.new(context: ctx, ctl_ds: ctl_ds, task_id: 1, force: false, logger: ctx.logger)
          assert_raise(JobDuplicated) {
            job.execute_task
          }
        }
      end

      test "execute_task (duplicated but forced)" do
        setup_context {|ctx, ctl_ds, db|
          db.insert_into 'strload_tables', [1, 'testschema.desttable', 'testschema', 'desttable', 100, 1800, false]
          db.insert_into 'strload_tasks', [11, 'streaming_load_v3', 1, current_timestamp]
          db.insert_into 'strload_task_objects', [11, 1001], [11, 1002]
          db.insert_into 'strload_objects',
            [1001, 's3://data-bucket/testschema.desttable/0001.json.gz', 1024, 'testschema.desttable', 'mmmm', current_timestamp, current_timestamp],
            [1002, 's3://data-bucket/testschema.desttable/0002.json.gz', 1024, 'testschema.desttable', 'mmmm', current_timestamp, current_timestamp]

          job = Job.new(context: ctx, ctl_ds: ctl_ds, task_id: 11, force: true, logger: ctx.logger)
          job.execute_task

          copy_stmt = "copy testschema.desttable from '#{job.manifest.url}' credentials 'cccc' manifest statupdate false compupdate false json 'auto' gzip timeformat 'auto' dateformat 'auto' acceptanydate acceptinvchars ' ' truncatecolumns trimblanks ;"
          assert_equal [copy_stmt], job.data_ds.sql_list

          job_row = db.query_row("select * from strload_jobs where job_id = #{job.job_id}")
          assert_equal 11, job_row['task_id'].to_i
          assert_equal job.process_id, job_row['process_id']
          assert_equal 'success', job_row['status']
          assert(/forced/ =~ job_row['message'])
        }
      end

      test "execute_task (load fails / first time)" do
        setup_context {|ctx, ctl_ds, db|
          db.insert_into 'strload_tables', [1, 'testschema.sql_fails', 'testschema', 'sql_fails', 100, 1800, false]
          db.insert_into 'strload_tasks', [11, 'streaming_load_v3', 1, current_timestamp]
          db.insert_into 'strload_task_objects', [11, 1001], [11, 1002]
          db.insert_into 'strload_objects',
            [1001, 's3://data-bucket/testschema.desttable/0001.json.gz', 1024, 'testschema.desttable', 'mmmm', current_timestamp, current_timestamp],
            [1002, 's3://data-bucket/testschema.desttable/0002.json.gz', 1024, 'testschema.desttable', 'mmmm', current_timestamp, current_timestamp]

          job = Job.new(context: ctx, ctl_ds: ctl_ds, task_id: 11, force: false, logger: ctx.logger)
          assert_raise(JobFailure) {
            job.execute_task
          }
          copy_stmt = "copy testschema.sql_fails from '#{job.manifest.url}' credentials 'cccc' manifest statupdate false compupdate false json 'auto' gzip timeformat 'auto' dateformat 'auto' acceptanydate acceptinvchars ' ' truncatecolumns trimblanks ;"
          assert_equal [copy_stmt], job.data_ds.sql_list

          job_row = db.query_row("select * from strload_jobs where job_id = #{job.job_id}")
          assert_equal 11, job_row['task_id'].to_i
          assert_equal job.process_id, job_row['process_id']
          assert_equal 'failure', job_row['status']
        }
      end

      test "execute_task (load fails / nth time)" do
        setup_context {|ctx, ctl_ds, db|
          db.insert_into 'strload_tables', [1, 'testschema.sql_fails', 'testschema', 'sql_fails', 100, 1800, false]
          db.insert_into 'strload_tasks', [11, 'streaming_load_v3', 1, current_timestamp]
          db.insert_into 'strload_task_objects', [11, 1001], [11, 1002]
          db.insert_into 'strload_objects',
            [1001, 's3://data-bucket/testschema.sql_fails/0001.json.gz', 1024, 'testschema.sql_fails', 'mmmm', current_timestamp, current_timestamp],
            [1002, 's3://data-bucket/testschema.sql_fails/0002.json.gz', 1024, 'testschema.sql_fails', 'mmmm', current_timestamp, current_timestamp]
          db.insert_into 'strload_jobs',
            [101, 11, 'localhost-1234', 'failure', current_timestamp, current_timestamp, 'query failed'],
            [102, 11, 'localhost-1234', 'failure', current_timestamp, current_timestamp, 'query failed']

          job = Job.new(context: ctx, ctl_ds: ctl_ds, task_id: 11, force: false, logger: ctx.logger)
          assert_raise(JobFailure) {
            job.execute_task
          }
          copy_stmt = "copy testschema.sql_fails from '#{job.manifest.url}' credentials 'cccc' manifest statupdate false compupdate false json 'auto' gzip timeformat 'auto' dateformat 'auto' acceptanydate acceptinvchars ' ' truncatecolumns trimblanks ;"
          assert_equal [copy_stmt], job.data_ds.sql_list

          job_row = db.query_row("select * from strload_jobs where job_id = #{job.job_id}")
          assert_equal 11, job_row['task_id'].to_i
          assert_equal job.process_id, job_row['process_id']
          assert_equal 'failure', job_row['status']
          assert(/retry\#2/ =~ job_row['message'])
        }
      end

      test "execute_task (too many retry)" do
        setup_context {|ctx, ctl_ds, db|
          db.insert_into 'strload_tables', [1, 'testschema.sql_fails', 'testschema', 'sql_fails', 100, 1800, false]
          db.insert_into 'strload_tasks', [11, 'streaming_load_v3', 1, current_timestamp]
          db.insert_into 'strload_task_objects', [11, 1001], [11, 1002]
          db.insert_into 'strload_objects',
            [1001, 's3://data-bucket/testschema.sql_fails/0001.json.gz', 1024, 'testschema.sql_fails', 'mmmm', current_timestamp, current_timestamp],
            [1002, 's3://data-bucket/testschema.sql_fails/0002.json.gz', 1024, 'testschema.sql_fails', 'mmmm', current_timestamp, current_timestamp]
          db.insert_into 'strload_jobs',
            [101, 11, 'localhost-1234', 'failure', current_timestamp, current_timestamp, 'query failed'],
            [102, 11, 'localhost-1234', 'failure', current_timestamp, current_timestamp, 'retry#1 query failed'],
            [103, 11, 'localhost-1234', 'failure', current_timestamp, current_timestamp, 'retry#2 query failed'],
            [104, 11, 'localhost-1234', 'failure', current_timestamp, current_timestamp, 'retry#3 query failed'],
            [105, 11, 'localhost-1234', 'failure', current_timestamp, current_timestamp, 'retry#4 query failed']

          job = Job.new(context: ctx, ctl_ds: ctl_ds, task_id: 11, force: false, logger: ctx.logger)
          assert_raise(JobCancelled) {
            job.execute_task
          }
          copy_stmt = "copy testschema.sql_fails from '#{job.manifest.url}' credentials 'cccc' manifest statupdate false compupdate false json 'auto' gzip timeformat 'auto' dateformat 'auto' acceptanydate acceptinvchars ' ' truncatecolumns trimblanks ;"
          assert_equal [copy_stmt], job.data_ds.sql_list
          job_row = db.query_row("select * from strload_jobs where job_id = #{job.job_id}")
          assert_equal 11, job_row['task_id'].to_i
          assert_equal job.process_id, job_row['process_id']
          assert_equal 'failure', job_row['status']
          assert(/retry\#5 FINAL/ =~ job_row['message'])
        }
      end

      test "execute_task (job error)" do
        setup_context {|ctx, ctl_ds, db|
          db.insert_into 'strload_tables', [1, 'testschema.job_error', 'testschema', 'job_error', 100, 1800, false]
          db.insert_into 'strload_tasks', [11, 'streaming_load_v3', 1, current_timestamp]
          db.insert_into 'strload_task_objects', [11, 1001], [11, 1002]
          db.insert_into 'strload_objects',
            [1001, 's3://data-bucket/testschema.job_error/0001.json.gz', 1024, 'testschema.job_error', 'mmmm', current_timestamp, current_timestamp],
            [1002, 's3://data-bucket/testschema.job_error/0002.json.gz', 1024, 'testschema.job_error', 'mmmm', current_timestamp, current_timestamp]

          job = Job.new(context: ctx, ctl_ds: ctl_ds, task_id: 11, force: false, logger: ctx.logger)
          assert_raise(JobError) {
            job.execute_task
          }
          assert_equal 1, job.data_ds.sql_list.size
          job_row = db.query_row("select * from strload_jobs where job_id = #{job.job_id}")
          assert_equal 11, job_row['task_id'].to_i
          assert_equal job.process_id, job_row['process_id']
          assert_equal 'error', job_row['status']
        }
      end

      test "execute_task (unexpected error)" do
        setup_context {|ctx, ctl_ds, db|
          db.insert_into 'strload_tables', [1, 'testschema.unexpected_error', 'testschema', 'unexpected_error', 100, 1800, false]
          db.insert_into 'strload_tasks', [11, 'streaming_load_v3', 1, sql('current_timestamp')]
          db.insert_into 'strload_task_objects', [11, 1001], [11, 1002]
          db.insert_into 'strload_objects',
            [1001, 's3://data-bucket/testschema.unexpected_error/0001.json.gz', 1024, 'testschema.unexpected_error', 'mmmm', current_timestamp, current_timestamp],
            [1002, 's3://data-bucket/testschema.unexpected_error/0002.json.gz', 1024, 'testschema.unexpected_error', 'mmmm', current_timestamp, current_timestamp]

          job = Job.new(context: ctx, ctl_ds: ctl_ds, task_id: 11, force: false, logger: ctx.logger)
          assert_raise(JobError) {
            job.execute_task
          }
          assert_equal 1, job.data_ds.sql_list.size
          job_row = db.query_row("select * from strload_jobs where job_id = #{job.job_id}")
          assert_equal 11, job_row['task_id'].to_i
          assert_equal job.process_id, job_row['process_id']
          assert_equal 'error', job_row['status']
        }
      end

      def setup_context(verbose: false)
        ctx = Context.for_application('.', environment: 'test', logger: (verbose ? nil : NullLogger.new))
        ctl_ds = ctx.get_data_source('sql', 'dwhctl')
        ctl_ds.open {|conn|
          client = SQLClient.new(conn)
          clear_all_tables(client)
          yield ctx, ctl_ds, client
        }
      end

      # FIXME: database cleaner
      def clear_all_tables(client)
        client.truncate_tables %w[
          strload_tables
          strload_objects
          strload_task_objects
          strload_tasks
          strload_jobs
        ]
      end

      class SQLClient
        include SQLUtils

        def initialize(conn)
          @conn = conn
        end

        extend Forwardable
        def_delegators '@conn',
          :query,
          :query_value,
          :query_values,
          :query_row,
          :select,
          :update

        def insert_into(table, *records)
          sql = "insert into #{table}"
          sep = ' values '
          records.each do |rec|
            sql << sep; sep = ', '
            sql << format_values(rec)
          end
          @conn.update(sql)
        end

        def truncate_tables(tables)
          tables.each do |name|
            @conn.update("truncate #{name}")
          end
        end

        private

        def format_values(rec)
          '(' + rec.map {|val| format_value(val) }.join(', ') + ')'
        end

        def format_value(val)
          case val
          when nil then 'null'
          when true, false then val.to_s
          when Integer then val.to_s
          when SQLExpr then val.to_s
          when String then sql_string_literal(val)
          else
            raise ArgumentError, "unsupported value type: #{val.class}: #{val.inspect}"
          end
        end

        module DSL
          def null
            nil
          end

          def current_timestamp
            SQLExpr.new('current_timestamp')
          end

          def sql(expr)
            SQLExpr.new(expr)
          end
        end

        class SQLExpr
          def initialize(expr)
            @expr = expr
          end

          def to_s
            @expr
          end
        end
      end

      include SQLClient::DSL

      class PSQLDataSourceMock < DataSource
        declare_type 'psql_mock'

        def initialize(fail_pattern: nil, error_pattern: nil, exception_pattern: nil, **params)
          @sql_list = []
          @fail_pattern = fail_pattern ? Regexp.compile(fail_pattern) : nil
          @error_pattern = error_pattern ? Regexp.compile(error_pattern) : nil
          @exception_pattern = exception_pattern ? Regexp.compile(exception_pattern) : nil
        end

        attr_reader :sql_list

        def open
          conn = Connection.new(self)
          if block_given?
            yield conn
          else
            conn
          end
        end

        def issue_sql(sql)
          logger.info "[mock] #{sql}"
          @sql_list.push sql
          if @fail_pattern and @fail_pattern =~ sql
            raise JobFailure, "query failed: #{sql}"
          end
          if @error_pattern and @error_pattern =~ sql
            raise JobError, "error"
          end
          if @exception_pattern and @exception_pattern =~ sql
            raise ArgumentError, "unexpected exception"
          end
        end

        class Connection
          def initialize(ds)
            @ds = ds
          end

          def execute(sql)
            @ds.issue_sql sql
          end

          def transaction
            @ds.issue_sql "begin transaction;"
            yield Transaction.new(@ds)
          end
        end

        class Transaction
          def initialize(ds)
            @ds = ds
          end

          def commit
            @ds.issue_sql "commit;"
          end

          def truncate_and_commit(table)
            @ds.issue_sql "truncate #{table};"
          end
        end
      end

      class S3DataSourceMock < DataSource
        declare_type 's3_mock'

        def initialize(**params)
        end

        def credential_string
          'cccc'
        end

        def url(name)
          "s3://bucket/prefix/#{name}"
        end

        def object(name)
          ObjectMock.new(url(name), logger)
        end

        class ObjectMock
          def initialize(url, logger)
            @url = url
            @logger = logger
          end

          def put(body:)
            @logger.info "[mock] S3 PUT #{@url} content=#{body[0,20].inspect}..."
          end

          def delete
            @logger.info "[mock] S3 DELETE #{@url}"
          end
        end
      end

    end

  end
end

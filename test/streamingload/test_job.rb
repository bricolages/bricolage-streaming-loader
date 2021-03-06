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
        setup_context {|db|
          db.insert_into 'strload_tables', [1, 'testschema.desttable', 'testschema', 'desttable', 100, 1800, false, 'test-bucket', 'test-prefix']
          db.insert_into 'strload_tasks', [1, 'streaming_load_v3', 1, current_timestamp]
          db.insert_into 'strload_task_objects', [1, 1], [1, 2]
          db.insert_into 'strload_objects',
            [1, 's3://data-bucket/testschema.desttable/0001.json.gz', 1024, 'testschema.desttable', 'mmmm', current_timestamp, current_timestamp],
            [2, 's3://data-bucket/testschema.desttable/0002.json.gz', 1024, 'testschema.desttable', 'mmmm', current_timestamp, current_timestamp]

          job = new_job(task_id: 1, force: false)
          job.execute_task

          assert_equal [
            "begin transaction;",
            "copy testschema.desttable from '#{job.manifest.url}' credentials 'cccc' manifest statupdate false compupdate false json 'auto' gzip timeformat 'auto' dateformat 'auto' acceptanydate acceptinvchars ' ' truncatecolumns trimblanks ;",
            "insert into strload_load_logs (task_id, job_id, finish_time) values (1, #{job.job_id}, current_timestamp)",
            "commit;"
          ], job.data_ds.sql_list

          job_row = db.query_row("select * from strload_jobs where job_id = #{job.job_id}")
          assert_equal 1, job_row['task_id'].to_i
          assert_equal job.process_id, job_row['process_id']
          assert_equal 'success', job_row['status']
        }
      end

      test "execute_task (with work table)" do
        setup_context {|db|
          db.insert_into 'strload_tables', [1, 'testschema.with_work_table', 'testschema', 'with_work_table', 100, 1800, false, 'test-bucket', 'test-prefix']
          db.insert_into 'strload_tasks', [11, 'streaming_load_v3', 1, current_timestamp]
          db.insert_into 'strload_task_objects', [11, 1001], [11, 1002]
          db.insert_into 'strload_objects',
            [1001, 's3://data-bucket/testschema.with_work_table/0001.json.gz', 1024, 'testschema.with_work_table', 'mmmm', current_timestamp, current_timestamp],
            [1002, 's3://data-bucket/testschema.with_work_table/0002.json.gz', 1024, 'testschema.with_work_table', 'mmmm', current_timestamp, current_timestamp]

          job = new_job(task_id: 11, force: false)
          job.execute_task

          assert_equal [
            "begin transaction;",
            "delete from testschema.with_work_table_wk",
            "copy testschema.with_work_table_wk from '#{job.manifest.url}' credentials 'cccc' manifest statupdate false compupdate false json 'auto' gzip timeformat 'auto' dateformat 'auto' acceptanydate acceptinvchars ' ' truncatecolumns trimblanks ;",
            "insert into testschema.with_work_table select * from testschema.with_work_table_wk;\n",
            "insert into strload_load_logs (task_id, job_id, finish_time) values (11, #{job.job_id}, current_timestamp)",
            "truncate testschema.with_work_table_wk;"
          ], job.data_ds.sql_list

          job_row = db.query_row("select * from strload_jobs where job_id = #{job.job_id}")
          assert_equal 11, job_row['task_id'].to_i
          assert_equal job.process_id, job_row['process_id']
          assert_equal 'success', job_row['status']
        }
      end

      test "execute_task (disabled)" do
        setup_context {|db|
          db.insert_into 'strload_tables', [1, 'testschema.desttable', 'testschema', 'desttable', 100, 1800, true, 'test-bucket', 'test-prefix']
          db.insert_into 'strload_tasks', [1, 'streaming_load_v3', 1, current_timestamp]

          job = new_job(task_id: 1, force: false)
          assert_raise(JobDefered) {
            job.execute_task
          }
          count = db.query_value("select count(*) from strload_jobs")
          assert_equal 0, count.to_i
        }
      end

      test "execute_task (duplicated)" do
        setup_context {|db|
          db.insert_into 'strload_tables', [1, 'testschema.desttable', 'testschema', 'desttable', 100, 1800, false, 'test-bucket', 'test-prefix']
          db.insert_into 'strload_tasks', [1, 'streaming_load_v3', 1, current_timestamp]
          db.insert_into 'strload_jobs',
            [1, 1, 'localhost-1234', 'failure', current_timestamp, current_timestamp, ''],
            [2, 1, 'localhost-1234', 'success', current_timestamp, current_timestamp, ''],
            [3, 1, 'localhost-1234', 'duplicated', current_timestamp, current_timestamp, '']

          job = new_job(task_id: 1, force: false)
          assert_raise(JobDuplicated) {
            job.execute_task
          }
        }
      end

      test "execute_task (duplicated but forced)" do
        setup_context {|db|
          db.insert_into 'strload_tables', [1, 'testschema.desttable', 'testschema', 'desttable', 100, 1800, false, 'test-bucket', 'test-prefix']
          db.insert_into 'strload_tasks', [11, 'streaming_load_v3', 1, current_timestamp]
          db.insert_into 'strload_task_objects', [11, 1001], [11, 1002]
          db.insert_into 'strload_objects',
            [1001, 's3://data-bucket/testschema.desttable/0001.json.gz', 1024, 'testschema.desttable', 'mmmm', current_timestamp, current_timestamp],
            [1002, 's3://data-bucket/testschema.desttable/0002.json.gz', 1024, 'testschema.desttable', 'mmmm', current_timestamp, current_timestamp]

          job = new_job(task_id: 11, force: true)
          job.execute_task

          assert_equal [
            "begin transaction;",
            "copy testschema.desttable from '#{job.manifest.url}' credentials 'cccc' manifest statupdate false compupdate false json 'auto' gzip timeformat 'auto' dateformat 'auto' acceptanydate acceptinvchars ' ' truncatecolumns trimblanks ;",
            "insert into strload_load_logs (task_id, job_id, finish_time) values (11, #{job.job_id}, current_timestamp)",
            "commit;"
          ], job.data_ds.sql_list

          job_row = db.query_row("select * from strload_jobs where job_id = #{job.job_id}")
          assert_equal 11, job_row['task_id'].to_i
          assert_equal job.process_id, job_row['process_id']
          assert_equal 'success', job_row['status']
          assert(/forced/ =~ job_row['message'])
        }
      end

      test "execute_task (load fails / first time)" do
        setup_context {|db|
          db.insert_into 'strload_tables', [1, 'testschema.sql_fails', 'testschema', 'sql_fails', 100, 1800, false, 'test-bucket', 'test-prefix']
          db.insert_into 'strload_tasks', [11, 'streaming_load_v3', 1, current_timestamp]
          db.insert_into 'strload_task_objects', [11, 1001], [11, 1002]
          db.insert_into 'strload_objects',
            [1001, 's3://data-bucket/testschema.desttable/0001.json.gz', 1024, 'testschema.desttable', 'mmmm', current_timestamp, current_timestamp],
            [1002, 's3://data-bucket/testschema.desttable/0002.json.gz', 1024, 'testschema.desttable', 'mmmm', current_timestamp, current_timestamp]

          job = new_job(task_id: 11, force: false)
          assert_raise(JobFailure) {
            job.execute_task
          }
          assert_equal [
            "begin transaction;",
            "copy testschema.sql_fails from '#{job.manifest.url}' credentials 'cccc' manifest statupdate false compupdate false json 'auto' gzip timeformat 'auto' dateformat 'auto' acceptanydate acceptinvchars ' ' truncatecolumns trimblanks ;",
            "abort;"
          ], job.data_ds.sql_list

          job_row = db.query_row("select * from strload_jobs where job_id = #{job.job_id}")
          assert_equal 11, job_row['task_id'].to_i
          assert_equal job.process_id, job_row['process_id']
          assert_equal 'failure', job_row['status']
        }
      end

      test "execute_task (load fails / nth time)" do
        setup_context {|db|
          db.insert_into 'strload_tables', [1, 'testschema.sql_fails', 'testschema', 'sql_fails', 100, 1800, false, 'test-bucket', 'test-prefix']
          db.insert_into 'strload_tasks', [11, 'streaming_load_v3', 1, current_timestamp]
          db.insert_into 'strload_task_objects', [11, 1001], [11, 1002]
          db.insert_into 'strload_objects',
            [1001, 's3://data-bucket/testschema.sql_fails/0001.json.gz', 1024, 'testschema.sql_fails', 'mmmm', current_timestamp, current_timestamp],
            [1002, 's3://data-bucket/testschema.sql_fails/0002.json.gz', 1024, 'testschema.sql_fails', 'mmmm', current_timestamp, current_timestamp]
          db.insert_into 'strload_jobs',
            [101, 11, 'localhost-1234', 'failure', current_timestamp, current_timestamp, 'query failed']

          job = new_job(task_id: 11, force: false)
          assert_raise(JobFailure) {
            job.execute_task
          }
          assert_equal [
            "begin transaction;",
            "copy testschema.sql_fails from '#{job.manifest.url}' credentials 'cccc' manifest statupdate false compupdate false json 'auto' gzip timeformat 'auto' dateformat 'auto' acceptanydate acceptinvchars ' ' truncatecolumns trimblanks ;",
            "abort;"
          ], job.data_ds.sql_list

          job_row = db.query_row("select * from strload_jobs where job_id = #{job.job_id}")
          assert_equal 11, job_row['task_id'].to_i
          assert_equal job.process_id, job_row['process_id']
          assert_equal 'failure', job_row['status']
          assert(/retry\#1/ =~ job_row['message'])
        }
      end

      test "execute_task (too many retry)" do
        setup_context {|db|
          db.insert_into 'strload_tables', [1, 'testschema.sql_fails', 'testschema', 'sql_fails', 100, 1800, false, 'test-bucket', 'test-prefix']
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

          job = new_job(task_id: 11, force: false)
          assert_raise(JobCancelled) {
            job.execute_task
          }
          assert_equal [
            "begin transaction;",
            "copy testschema.sql_fails from '#{job.manifest.url}' credentials 'cccc' manifest statupdate false compupdate false json 'auto' gzip timeformat 'auto' dateformat 'auto' acceptanydate acceptinvchars ' ' truncatecolumns trimblanks ;",
            "abort;"
          ], job.data_ds.sql_list

          job_row = db.query_row("select * from strload_jobs where job_id = #{job.job_id}")
          assert_equal 11, job_row['task_id'].to_i
          assert_equal job.process_id, job_row['process_id']
          assert_equal 'failure', job_row['status']
          assert(/retry\#5 FINAL/ =~ job_row['message'])
        }
      end

      test "execute_task (job error)" do
        setup_context {|db|
          db.insert_into 'strload_tables', [1, 'testschema.job_error', 'testschema', 'job_error', 100, 1800, false, 'test-bucket', 'test-prefix']
          db.insert_into 'strload_tasks', [11, 'streaming_load_v3', 1, current_timestamp]
          db.insert_into 'strload_task_objects', [11, 1001], [11, 1002]
          db.insert_into 'strload_objects',
            [1001, 's3://data-bucket/testschema.job_error/0001.json.gz', 1024, 'testschema.job_error', 'mmmm', current_timestamp, current_timestamp],
            [1002, 's3://data-bucket/testschema.job_error/0002.json.gz', 1024, 'testschema.job_error', 'mmmm', current_timestamp, current_timestamp]

          job = new_job(task_id: 11, force: false)
          assert_raise(JobError) {
            job.execute_task
          }
          assert_equal [
            "begin transaction;",
            "copy testschema.job_error from '#{job.manifest.url}' credentials 'cccc' manifest statupdate false compupdate false json 'auto' gzip timeformat 'auto' dateformat 'auto' acceptanydate acceptinvchars ' ' truncatecolumns trimblanks ;",
            "abort;"
          ], job.data_ds.sql_list

          job_row = db.query_row("select * from strload_jobs where job_id = #{job.job_id}")
          assert_equal 11, job_row['task_id'].to_i
          assert_equal job.process_id, job_row['process_id']
          assert_equal 'error', job_row['status']
        }
      end

      test "execute_task (unexpected error)" do
        setup_context {|db|
          db.insert_into 'strload_tables', [1, 'testschema.unexpected_error', 'testschema', 'unexpected_error', 100, 1800, false, 'test-bucket', 'test-prefix']
          db.insert_into 'strload_tasks', [11, 'streaming_load_v3', 1, current_timestamp]
          db.insert_into 'strload_task_objects', [11, 1001], [11, 1002]
          db.insert_into 'strload_objects',
            [1001, 's3://data-bucket/testschema.unexpected_error/0001.json.gz', 1024, 'testschema.unexpected_error', 'mmmm', current_timestamp, current_timestamp],
            [1002, 's3://data-bucket/testschema.unexpected_error/0002.json.gz', 1024, 'testschema.unexpected_error', 'mmmm', current_timestamp, current_timestamp]

          job = new_job(task_id: 11, force: false)
          assert_raise(JobError) {
            job.execute_task
          }
          assert_equal [
            "begin transaction;",
            "copy testschema.unexpected_error from '#{job.manifest.url}' credentials 'cccc' manifest statupdate false compupdate false json 'auto' gzip timeformat 'auto' dateformat 'auto' acceptanydate acceptinvchars ' ' truncatecolumns trimblanks ;",
            "abort;"
          ], job.data_ds.sql_list

          job_row = db.query_row("select * from strload_jobs where job_id = #{job.job_id}")
          assert_equal 11, job_row['task_id'].to_i
          assert_equal job.process_id, job_row['process_id']
          assert_equal 'error', job_row['status']
        }
      end

      test "execute_task (load error)" do
        setup_context {|db|
          db.insert_into 'strload_tables', [1, 'testschema.load_error', 'testschema', 'load_error', 100, 1800, false, 'test-bucket', 'test-prefix']
          db.insert_into 'strload_tasks', [11, 'streaming_load_v3', 1, current_timestamp]
          db.insert_into 'strload_task_objects', [11, 1001], [11, 1002]
          db.insert_into 'strload_objects',
            [1001, 's3://data-bucket/testschema.desttable/0001.json.gz', 1024, 'testschema.desttable', 'mmmm', current_timestamp, current_timestamp],
            [1002, 's3://data-bucket/testschema.desttable/0002.json.gz', 1024, 'testschema.desttable', 'mmmm', current_timestamp, current_timestamp]

          job = new_job(task_id: 11, force: false)
          assert_raise(JobError) {
            job.execute_task
          }
          assert_equal [
            "begin transaction;",
            "copy testschema.load_error from '#{job.manifest.url}' credentials 'cccc' manifest statupdate false compupdate false json 'auto' gzip timeformat 'auto' dateformat 'auto' acceptanydate acceptinvchars ' ' truncatecolumns trimblanks ;",
            "abort;"
          ], job.data_ds.sql_list

          job_row = db.query_row("select * from strload_jobs where job_id = #{job.job_id}")
          assert_equal 11, job_row['task_id'].to_i
          assert_equal job.process_id, job_row['process_id']
          assert_equal 'error', job_row['status']
          assert(/stl_load_errors/ =~ job_row['message'])
        }
      end

      test "execute_task (unknown status, really=success)" do
        setup_context {|db|
          db.insert_into 'strload_tables', [1, 'testschema.desttable', 'testschema', 'desttable', 100, 1800, false, 'test-bucket', 'test-prefix']
          db.insert_into 'strload_tasks', [11, 'streaming_load_v3', 1, current_timestamp]
          db.insert_into 'strload_jobs',
            [101, 11, 'localhost-1234', 'unknown', current_timestamp, current_timestamp, 'data connection failed']
          @data_ds.provide_job_status 101, true

          job = new_job(task_id: 11, force: false)
          assert_raise(JobDuplicated) {
            job.execute_task
          }

          job_row = db.query_row("select * from strload_jobs where job_id = 101")
          assert_equal 'success', job_row['status']
        }
      end

      test "execute_task (unknown status, really=failure)" do
        setup_context {|db|
          db.insert_into 'strload_tables', [1, 'testschema.desttable', 'testschema', 'desttable', 100, 1800, false, 'test-bucket', 'test-prefix']
          db.insert_into 'strload_tasks', [11, 'streaming_load_v3', 1, current_timestamp]
          db.insert_into 'strload_task_objects', [11, 1001], [11, 1002]
          db.insert_into 'strload_objects',
            [1001, 's3://data-bucket/testschema.desttable/0001.json.gz', 1024, 'testschema.desttable', 'mmmm', current_timestamp, current_timestamp],
            [1002, 's3://data-bucket/testschema.desttable/0002.json.gz', 1024, 'testschema.desttable', 'mmmm', current_timestamp, current_timestamp]
          db.insert_into 'strload_jobs',
            [101, 11, 'localhost-1234', 'unknown', current_timestamp, current_timestamp, 'data connection failed']
          @data_ds.provide_job_status 101, false

          job = new_job(task_id: 11, force: false)
          job.execute_task

          assert_equal [
            "begin transaction;",
            "copy testschema.desttable from '#{job.manifest.url}' credentials 'cccc' manifest statupdate false compupdate false json 'auto' gzip timeformat 'auto' dateformat 'auto' acceptanydate acceptinvchars ' ' truncatecolumns trimblanks ;",
            "insert into strload_load_logs (task_id, job_id, finish_time) values (11, #{job.job_id}, current_timestamp)",
            "commit;"
          ], job.data_ds.sql_list

          job_row = db.query_row("select * from strload_jobs where job_id = 101")
          assert_equal 'failure', job_row['status']

          job_row = db.query_row("select * from strload_jobs where job_id = #{job.job_id}")
          assert_equal 11, job_row['task_id'].to_i
          assert_equal job.process_id, job_row['process_id']
          assert_equal 'success', job_row['status']
        }
      end

      def setup_context(verbose: false)
        @ctx = Context.for_application('.', environment: 'test', logger: (verbose ? nil : NullLogger.new))
        @ctl_ds = @ctx.get_data_source('sql', 'dwhctl')
        @data_ds = @ctx.get_data_source('sql', 'db_data_mock')
        @ctl_ds.open {|conn|
          client = SQLClient.new(conn)
          clear_all_tables(client)
          yield client
        }
      end

      def new_job(task_id:, force:)
        Job.new(
          context: @ctx,
          ctl_ds: @ctl_ds,
          data_ds: @data_ds,
          logger: @ctx.logger,
          task_id: task_id,
          force: force
        )
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

        def initialize(fail_pattern: nil, error_pattern: nil, exception_pattern: nil, load_error_pattern: nil, **params)
          @sql_list = []
          @fail_pattern = fail_pattern ? Regexp.compile(fail_pattern) : nil
          @error_pattern = error_pattern ? Regexp.compile(error_pattern) : nil
          @exception_pattern = exception_pattern ? Regexp.compile(exception_pattern) : nil
          @load_error_pattern = load_error_pattern ? Regexp.compile(load_error_pattern) : nil
          @job_status = {}
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
          if @load_error_pattern and @load_error_pattern =~ sql
            raise JobError, "Load into table 'xxxx_table' failed.  Check 'stl_load_errors' system table for details."
          end
        end

        def provide_job_status(job_id, succeeded)
          @job_status[job_id] = succeeded
        end

        def job_succeeded?(job_id)
          raise "job status unregistered: job_id=#{job_id}" unless @job_status.key?(job_id)
          @job_status[job_id]
        end

        class Connection
          def initialize(ds)
            @ds = ds
          end

          def query_value(sql)
            case sql
            when /\bstrload_load_logs where job_id = (\d+)/
              job_id = $1.to_i
              @ds.job_succeeded?(job_id) ? 1 : 0
            else
              raise "unknown query: #{sql}"
            end
          end

          def execute(sql)
            @ds.issue_sql sql
          end

          def transaction
            @ds.issue_sql "begin transaction;"
            txn = Transaction.new(@ds)
            yield txn
          rescue
            txn.abort unless txn.committed?
            raise
          ensure
            txn.commit unless txn.committed?
          end
        end

        class Transaction
          def initialize(ds)
            @ds = ds
            @commit = false
          end

          def committed?
            @commit
          end

          def commit
            @ds.issue_sql "commit;"
            @commit = true
          end

          def abort
            @ds.issue_sql "abort;"
            @commit = true
          end

          def truncate_and_commit(table)
            @ds.issue_sql "truncate #{table};"
            @commit = true
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

      test "TaskInfo#failure_count" do
        test_data = [
          [%w[], 0],
          [%w[success], 0],
          [%w[failure], 1],
          [%w[error], 1],
          [%w[failure failure], 2],
          [%w[failure error], 2],
          [%w[failure success], 0],
          [%w[success success], 0],
          [%w[failure success failure], 1],
          [%w[failure success failure success failure failure], 2]
        ]
        c = Job::ControlConnection
        test_data.each do |status_list, expected_count|
          task = c::TaskInfo.new(nil,nil,nil,nil,nil,nil, status_list.map {|st| c::JobInfo.new(nil, st) })
          assert_equal expected_count, task.failure_count
        end
      end

    end   # class TestJob

  end   # module StreamingLoad
end   # module Bricolage

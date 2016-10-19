require 'bricolage/streamingload/jobparams'
require 'bricolage/streamingload/manifest'
require 'bricolage/sqlutils'
require 'socket'
require 'json'

module Bricolage

  module StreamingLoad

    class JobCancelled < ApplicationError; end
    class JobDefered < ApplicationError; end
    class JobDuplicated < ApplicationError; end

    class ControlConnectionFailed < JobFailure; end
    class DataConnectionFailed < JobFailure; end


    class Job

      def initialize(context:, ctl_ds:, task_id:, force: false, logger:)
        @context = context
        @ctl_ds = ctl_ds
        @task_id = task_id
        @force = force
        @logger = logger

        @task = nil
        @job_id = nil
        @data_ds = nil
        @manifest = nil
      end

      # For tests
      attr_reader :job_id
      attr_reader :process_id
      attr_reader :task
      attr_reader :data_ds
      attr_reader :manifest

      # Returns true -> Deletes a SQS message
      # Returns false -> Keeps a SQS message
      def execute(fail_fast: false)
        execute_task
        return true
      rescue JobCancelled
        return true
      rescue JobDuplicated
        return true
      rescue JobDefered
        return false
      rescue ControlConnectionFailed => ex
        @logger.error ex.message
        wait_for_connection('ctl', @ctl_ds) unless fail_fast
        return false
      rescue DataConnectionFailed
        wait_for_connection('data', @data_ds) unless fail_fast
        # FIXME: tmp: We don't know the transaction was succeeded or not in the Redshift, auto-retry is too dangerous.
        #return false
        return true
      rescue JobFailure
        return false
      rescue JobError
        return true
      rescue Exception => ex
        @logger.exception ex
        return true
      end

      MAX_RETRY = 5

      def execute_task
        @process_id = "#{Socket.gethostname}-#{$$}"
        @logger.info "execute task: task_id=#{@task_id} force=#{@force} process_id=#{@process_id}"
        ctl = ControlConnection.new(@ctl_ds, @logger)

        ctl.open {
          @task = ctl.load_task(@task_id)
          @logger.info "task details: task_id=#{@task_id} table=#{@task.schema_name}.#{@task.table_name}"
          if @task.disabled
            # We do not record disabled job in the DB.
            @logger.info "task is disabled; defer task: task_id=#{@task_id}"
            raise JobDefered, "defered: task_id=#{@task_id}"
          end

          @job_id = ctl.begin_job(@task_id, @process_id, @force)
          unless @job_id
            @logger.warn "task is already succeeded and not forced; discard task: task_id=#{@task_id}"
            ctl.commit_duplicated_job @task_id, @process_id
            raise JobDuplicated, "duplicated: task_id=#{@task_id}"
          end
        }

        begin
          do_load @task, @job_id
          ctl.open {
            ctl.commit_job @job_id, (@force ? 'forced' : nil)
          }
        rescue ControlConnectionFailed
          raise

        # FIXME: tmp: should be a failure, not an error.
        rescue DataConnectionFailed => ex
          @logger.error ex.message
          ctl.open {
            ctl.abort_job job_id, 'error', ex.message.lines.first.strip
          }
          raise

        rescue JobFailure => ex
          @logger.error ex.message
          ctl.open {
            fail_count = ctl.fail_count(@task_id)
            final_retry = (fail_count >= MAX_RETRY)
            retry_msg = (fail_count > 0) ? "(retry\##{fail_count}#{final_retry ? ' FINAL' : ''}) " : ''
            ctl.abort_job job_id, 'failure', retry_msg + ex.message.lines.first.strip
            raise JobCancelled, "retry count exceeds limit: task_id=#{@task_id}" if final_retry
          }
          raise
        rescue JobError => ex
          @logger.error ex.message
          ctl.open {
            ctl.abort_job job_id, 'error', ex.message.lines.first.strip
          }
          raise
        rescue Exception => ex
          @logger.exception ex
          ctl.open {
            ctl.abort_job job_id, 'error', ex.message.lines.first.strip
          }
          raise JobError, "#{ex.class}: #{ex.message}"
        end
      end

      def do_load(task, job_id)
        params = JobParams.load(@context, task.task_class, task.schema_name, task.table_name)
        @data_ds = params.ds
        @manifest = ManifestFile.create(ds: params.ctl_bucket, job_id: job_id, object_urls: task.object_urls, logger: @logger)
        DataConnection.open(params.ds, @logger) {|data|
          if params.enable_work_table?
            data.load_with_work_table params.work_table, @manifest, params.load_options_string, params.sql_source
          else
            data.load_objects params.dest_table, @manifest, params.load_options_string
          end
        }
      end

      def wait_for_connection(type, ds)
        @logger.warn "loader: #{type} DB connection lost; polling..."
        start_time = Time.now
        n = 0
        while true
          begin
            ds.open {}
            @logger.warn "loader: #{type} DB connection recovered; return to normal state"
            return true
          rescue ConnectionError
          end
          sleep 15
          n += 1
          if n == 120  # 30 min
            # Could not get a connection in 30 minutes, now we return to the queue loop.
            # Next job may fail too, but we should not stop to receive the task queue too long,
            # because it contains control tasks.
            @logger.warn "loader: #{type} DB connection still failing (since #{start_time}); give up."
            return false
          end
        end
      end


      class DataConnection

        include SQLUtils

        def DataConnection.open(ds, logger = ds.logger, &block)
          new(ds, logger).open(&block)
        end

        def initialize(ds, logger = ds.logger)
          @ds = ds
          @connection = nil
          @logger = logger
        end

        def open(&block)
          @ds.open {|conn|
            @connection = conn
            yield self
          }
        rescue ConnectionError => ex
          raise DataConnectionFailed, "data connection failed: #{ex.message}"
        end

        def load_with_work_table(work_table, manifest, options, sql_source)
          @connection.transaction {|txn|
            # NOTE: This transaction ends with truncation, this DELETE does nothing
            # from the second time.  So don't worry about DELETE cost here.
            @connection.execute("delete from #{work_table}")
            load_objects work_table, manifest, options
            @connection.execute sql_source
            txn.truncate_and_commit work_table
          }
        end

        def load_objects(dest_table, manifest, options)
          @connection.execute(<<-EndSQL.strip.gsub(/\s+/, ' '))
              copy #{dest_table}
              from #{s manifest.url}
              credentials #{s manifest.credential_string}
              manifest
              statupdate false
              compupdate false
              #{options}
              ;
          EndSQL
          @logger.info "load succeeded: #{manifest.url}"
        end

      end   # class DataConnection


      class ControlConnection

        include SQLUtils

        def ControlConnection.open(ds, logger = ds.logger, &block)
          new(ds, logger).open(&block)
        end

        def initialize(ds, logger = ds.logger)
          @ds = ds
          @connection = nil
        end

        def open(&block)
          @ds.open {|conn|
            @connection = conn
            yield self
          }
        rescue ConnectionError => ex
          raise ControlConnectionFailed, "control connection failed: #{ex.message}"
        end

        TaskInfo = Struct.new(:task_id, :task_class, :schema_name, :table_name, :disabled, :object_urls)

        def load_task(task_id)
          rec = @connection.query_row(<<-EndSQL) or raise JobError, "no such task: #{task_id}"
            select
                tsk.task_class
                , tbl.schema_name
                , tbl.table_name
                , tbl.disabled
            from
                strload_tasks tsk
                inner join strload_tables tbl using (table_id)
            where
                tsk.task_id = #{task_id}
            ;
          EndSQL
          TaskInfo.new(
            task_id,
            rec['task_class'],
            rec['schema_name'],
            rec['table_name'],
            (rec['disabled'] != 'f'),
            load_object_urls(task_id)
          )
        end

        def load_object_urls(task_id)
          urls = @connection.query_values(<<-EndSQL)
            select
                o.object_url
            from
                strload_tasks t
                inner join strload_task_objects tob using (task_id)
                inner join strload_objects o using (object_id)
            where
                t.task_id = #{task_id}
            ;
          EndSQL
          urls
        end

        def begin_job(task_id, process_id, force)
          job_id = @connection.query_value(<<-EndSQL)
            insert into strload_jobs
                ( task_id
                , process_id
                , status
                , start_time
                )
            select
                task_id
                , #{s process_id}
                , 'running'
                , current_timestamp
            from
                strload_tasks
            where
                task_id = #{task_id}
                and (#{force ? 'true' : 'false'} or task_id not in (select task_id from strload_jobs where status = 'success'))
            returning job_id
            ;
          EndSQL
          return job_id ? job_id.to_i : nil
        end

        def fail_count(task_id)
          statuses = @connection.query_values(<<-EndSQL)
            select
                j.status
            from
                strload_tasks t
                inner join strload_jobs j using (task_id)
            where
                t.task_id = #{task_id}
            order by
                j.job_id desc
          EndSQL
          statuses.shift if statuses.first == 'running'   # current job
          statuses.take_while {|st| %w[failure error].include?(st) }.size
        end

        def commit_job(job_id, message = nil)
          @connection.transaction {|txn|
            write_job_result job_id, 'success', (message || '')
            update_loaded_flag job_id
          }
        end

        def abort_job(job_id, status, message)
          write_job_result(job_id, status, message)
        end

        MAX_MESSAGE_LENGTH = 1000

        def write_job_result(job_id, status, message)
          @connection.execute(<<-EndSQL)
            update
                strload_jobs
            set
                (status, finish_time, message) = (#{s status}, current_timestamp, #{s message[0, MAX_MESSAGE_LENGTH]})
            where
                job_id = #{job_id}
            ;
          EndSQL
        end

        def update_loaded_flag(job_id)
          @connection.execute(<<-EndSQL)
            update
                strload_objects
            set
                loaded = true
            where
                object_id in (
                  select
                      object_id
                  from
                      strload_task_objects
                  where task_id = (select task_id from strload_jobs where job_id = #{job_id})
                )
            ;
          EndSQL
        end

        def commit_duplicated_job(task_id, process_id)
          job_id = @connection.query_value(<<-EndSQL)
            insert into strload_jobs
                ( task_id
                , process_id
                , status
                , start_time
                , finish_time
                , message
                )
            select
                #{task_id}
                , #{s process_id}
                , 'duplicated'
                , current_timestamp
                , current_timestamp
                , ''
            returning job_id
            ;
          EndSQL
          return job_id
        end

      end   # class ControlConnection

    end   # class Job

  end   # module StreamingLoad

end   # module Bricolage

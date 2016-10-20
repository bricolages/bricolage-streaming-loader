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

      def initialize(context:, ctl_ds:, data_ds:, log_table: 'strload_load_logs', task_id:, force: false, logger:)
        @context = context
        @ctl_ds = ctl_ds
        @data_ds = data_ds
        @log_table = log_table
        @task_id = task_id
        @force = force
        @logger = logger

        @task = nil
        @job_id = nil
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
      rescue DataConnectionFailed => ex
        @logger.error ex.message
        wait_for_connection('data', @data_ds) unless fail_fast
        return false
      rescue JobFailure => ex
        @logger.error ex.message
        return false
      rescue JobError => ex
        @logger.error ex.message
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

          if @task.unknown_state?
            true_status = DataConnection.open(@data_ds, @logger) {|data|
              data.get_job_status(@log_table, @task.last_job_id)
            }
            @logger.info "fixiating unknown job status: job_id=#{@task.last_job_id}, status=(unknown->#{true_status})"
            @task.fix_last_job_status true_status
            ctl.fix_job_status @task.last_job_id, true_status
            @logger.info "job status fixed."
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
        rescue DataConnectionFailed => ex
          ctl.open {
            ctl.abort_job job_id, 'unknown', ex.message.lines.first.strip
          }
          raise
        rescue JobFailure => ex
          ctl.open {
            fail_count = @task.failure_count
            final_retry = (fail_count >= MAX_RETRY)
            retry_msg = (fail_count > 0) ? "(retry\##{fail_count}#{final_retry ? ' FINAL' : ''}) " : ''
            ctl.abort_job job_id, 'failure', retry_msg + ex.message.lines.first.strip
            raise JobCancelled, "retry count exceeds limit: task_id=#{@task_id}" if final_retry
          }
          raise
        rescue JobError => ex
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
            data.load_with_work_table params.work_table, @manifest, params.load_options_string, params.sql_source, @log_table, job_id
          else
            data.load_objects params.dest_table, @manifest, params.load_options_string, @log_table, job_id
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

        def get_job_status(log_table, job_id)
          count = @connection.query_value("select count(*) from #{log_table} where job_id = #{job_id}")
          count.to_i > 0 ? 'success' : 'failure'
        end

        def load_with_work_table(work_table, manifest, options, sql_source, log_table, job_id)
          @connection.transaction {|txn|
            # NOTE: This transaction ends with truncation, this DELETE does nothing
            # from the second time.  So don't worry about DELETE cost here.
            @connection.execute("delete from #{work_table}")
            execute_copy work_table, manifest, options
            @connection.execute sql_source
            write_load_log log_table, job_id
            txn.truncate_and_commit work_table
          }
        end

        def load_objects(dest_table, manifest, options, log_table, job_id)
          @connection.transaction {|txn|
            execute_copy dest_table, manifest, options
            write_load_log log_table, job_id
          }
        end

        def execute_copy(dest_table, manifest, options)
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

        def write_load_log(log_table, job_id)
          @connection.execute("insert into #{log_table} (job_id, finish_time) values (#{job_id}, current_timestamp)")
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

        TaskInfo = Struct.new(:task_id, :task_class, :schema_name, :table_name, :disabled, :object_urls, :jobs)
        class TaskInfo
          def unknown_state?
            return false if jobs.empty?
            jobs.last.status == 'unknown'
          end

          def last_job_id
            return nil if jobs.empty?
            jobs.last.job_id
          end

          def fix_last_job_status(st)
            jobs.last.status = st unless jobs.empty?
          end

          def failure_count
            @failure_count ||= begin
              statuses = jobs.map(&:status)
              statuses.delete('duplicated')
              last_succ = statuses.rindex('success')
              statuses[0..last_succ] = [] if last_succ
              statuses.size
            end
          end
        end

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
            load_object_urls(task_id),
            load_jobs(task_id)
          )
        end

        def load_jobs(task_id)
          records = @connection.query_rows(<<-EndSQL)
            select
                job_id
                , status
            from
                strload_jobs
            where
                task_id = #{task_id}
            order by
                start_time
            ;
          EndSQL
          records.map {|rec| JobInfo.new(rec['job_id'].to_i, rec['status']) }
        end

        JobInfo = Struct.new(:job_id, :status, :start_time)

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

        def fix_job_status(job_id, status)
          @connection.update(<<-EndSQL)
            update
                strload_jobs
            set
                status = #{s status}
                , message = 'status fixed: ' || message
            where
                job_id = #{job_id}
                and status = 'unknown'
            ;
          EndSQL
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

require 'bricolage/streamingload/loaderparams'
require 'bricolage/streamingload/manifest'
require 'bricolage/sqlutils'
require 'socket'
require 'json'

module Bricolage

  module StreamingLoad

    class Loader

      include SQLUtils

      def Loader.load_from_file(ctx, ctl_ds, task, logger:)
        params = LoaderParams.load(ctx, task)
        new(ctl_ds, params, logger: logger)
      end

      def initialize(ctl_ds, params, logger:)
        @ctl_ds = ctl_ds
        @params = params
        @logger = logger
        @process_id = "#{Socket.gethostname}-#{$$}"
      end

      def execute
        @job_id = assign_task
        return unless @job_id # task already executed by other loader
        @params.ds.open {|conn|
          @connection = conn
          do_load
        }
      end

      def assign_task
        @ctl_ds.open {|conn|
          job_id = conn.query_value(<<-EndSQL)
            insert into strload_jobs
                ( task_id
                , process_id
                , status
                , start_time
                )
            select
                task_id
                , #{s @process_id}
                , 'running'
                , current_timestamp
            from
                strload_tasks
            where
                task_id = #{@params.task_id}
                and (#{@params.force?} or task_id not in (select task_id from strload_jobs))
            returning job_id
            ;
          EndSQL
          return job_id
        }
      end

      def do_load
        manifest = ManifestFile.create(@params.ctl_bucket, job_id: @job_id, object_urls: @params.object_urls, logger: @logger)
        if @params.enable_work_table?
          @connection.transaction {|txn|
            # NOTE: This transaction ends with truncation, this DELETE does nothing
            # from the second time.  So don't worry about DELETE cost here.
            @connection.execute("delete from #{@params.work_table}")
            load_objects @params.work_table, manifest, @params.load_options_string
            commit_work_table txn, @params
          }
          commit_job_result
        else
          load_objects @params.dest_table, manifest, @params.load_options_string
          commit_job_result
        end
      rescue JobFailure => ex
        write_job_error 'failure', ex.message
        raise
      rescue Exception => ex
        write_job_error 'error', ex.message
        raise
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

      def commit_work_table(txn, params)
        @connection.execute(params.sql_source)
        txn.truncate_and_commit(params.work_table)
      end

      def commit_job_result
        @end_time = Time.now
        @ctl_ds.open {|conn|
          conn.transaction {
            write_job_result conn, 'success', ''
            update_loaded_flag conn
          }
        }
      end

      def update_loaded_flag(connection)
        connection.execute(<<-EndSQL)
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
                where task_id = (select task_id from strload_jobs where job_id = #{@job_id})
              )
          ;
        EndSQL
      end

      MAX_MESSAGE_LENGTH = 1000

      def write_job_error(status, message)
        @end_time = Time.now
        @logger.warn message.lines.first
        @ctl_ds.open {|conn|
          write_job_result conn, status, message.lines.first.strip[0, MAX_MESSAGE_LENGTH]
        }
      end

      def write_job_result(connection, status, message)
        connection.execute(<<-EndSQL)
          update
              strload_jobs
          set
              (status, finish_time, message) = (#{s status}, current_timestamp, #{s message})
          where
              job_id = #{@job_id}
          ;
        EndSQL
      end

    end

  end

end

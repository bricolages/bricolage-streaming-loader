require 'bricolage/context'
require 'bricolage/sqsdatasource'
require 'bricolage/streamingload/task'
require 'bricolage/streamingload/job'
require 'bricolage/streamingload/alertinglogger'
require 'bricolage/logger'
require 'bricolage/exception'
require 'bricolage/version'
require 'yaml'
require 'optparse'

module Bricolage

  module StreamingLoad

    class TaskHandler < SQSDataSource::MessageHandler

      def TaskHandler.main
        opts = TaskHandlerOptions.new(ARGV)
        opts.parse
        unless opts.rest_arguments.size <= 1
          $stderr.puts opts.usage
          exit 1
        end
        config_path = opts.rest_arguments[0] || "#{opts.working_dir}/config/#{opts.environment}/streamingload.yml"
        config = YAML.load(File.read(config_path))

        logger = opts.log_file_path ? new_logger(opts.log_file_path, config) : nil
        ctx = Context.for_application(opts.working_dir, environment: opts.environment, logger: logger)

        ctl_ds = ctx.get_data_source('sql', config.fetch('ctl-postgres-ds', 'db_ctl'))
        data_ds = ctx.get_data_source('sql', config.fetch('redshift-ds', 'db_data'))
        task_queue = ctx.get_data_source('sqs', config.fetch('task-queue-ds', 'sqs_task'))
        log_table = config.fetch('log-table', 'strload_load_logs')
        service_logger =
          if config.key?('alert-level')
            new_alerting_logger(ctx, config)
          else
            ctx.logger
          end

        task_handler = new(
          context: ctx,
          ctl_ds: ctl_ds,
          data_ds: data_ds,
          log_table: log_table,
          task_queue: task_queue,
          working_dir: opts.working_dir,
          logger: service_logger,
          job_class: opts.noop? ? NoopJob : Job
        )

        if opts.task_id
          # Single task mode
          task_handler.execute_task_by_id opts.task_id, force: opts.force?
        else
          # Server mode
          Process.daemon(true) if opts.daemon?
          Dir.chdir '/'
          create_pid_file opts.pid_file_path if opts.pid_file_path
          begin
            service_logger.info "*** bricolage-streaming-loader started: pid=#{$$}"
            task_handler.event_loop
            service_logger.info "*** bricolage-streaming-loader shutdown gracefully: pid=#{$$}"
          rescue Exception => ex
            service_logger.exception(ex)
            service_logger.error "*** bricolage-streaming-loader abort: pid=#{$$}"
            raise
          end
        end
      end

      def TaskHandler.new_logger(path, config)
        Logger.new(
          device: path,
          rotation_period: config.fetch('log-rotation-period', 'daily'),
          rotation_size: config.fetch('log-rotation-size', nil)
        )
      end

      def TaskHandler.new_alerting_logger(ctx, config)
        AlertingLogger.new(
          logger: ctx.logger,
          sns_datasource: ctx.get_data_source('sns', config.fetch('sns-ds', 'sns')),
          alert_level: config.fetch('alert-level', 'warn')
        )
      end

      def TaskHandler.create_pid_file(path)
        File.open(path, 'w') {|f|
          f.puts $$
        }
      rescue
        # ignore
      end

      def initialize(context:, ctl_ds:, data_ds:, log_table:, task_queue:, working_dir:, logger:, job_class: Job)
        @ctx = context
        @ctl_ds = ctl_ds
        @data_ds = data_ds
        @log_table = log_table
        @task_queue = task_queue
        @working_dir = working_dir
        @logger = logger
        @job_class = job_class
      end

      attr_reader :logger

      def execute_task_by_id(task_id, force: false)
        job = new_job(task_id, force)
        job.execute(fail_fast: true)
      end

      def event_loop
        @task_queue.handle_messages(handler: self, message_class: Task)
      end

      # message handler
      def handle_unknown(t)
        @logger.warn "unknown task: #{t.message_body}"
        @task_queue.delete_message t
      end

      # message handler
      def handle_streaming_load_v3(t)
        Dir.chdir(@working_dir) {
          job = new_job(t.task_id, t.force?)
          if job.execute
            @task_queue.delete_message(t)
          end
        }
      rescue Exception => ex
        @logger.exception ex
      end

      def new_job(task_id, force)
        @job_class.new(context: @ctx, ctl_ds: @ctl_ds, data_ds: data_ds, log_table: @log_table, task_id: task_id, force: force, logger: @logger)
      end

      def job_class
        @job_class ||= Job
      end

      attr_writer :job_class

    end


    class NoopJob

      def initialize(context:, ctl_ds:, task_id:, force: false, logger:)
        @ctx = context
        @ctl_ds = ctl_ds
        @task_id = task_id
        @force = force
        @logger = logger
      end

      def execute(fail_fast: false)
        @logger.info "execute: fail_fast=#{fail_fast}"
        execute_task
        true
      end

      def execute_task
        @logger.info "execute_task: task_id=#{@task_id} force=#{@force} ctx=#{@ctx.home_path} ctl_ds=#{@ctl_ds.name} dir=#{@working_dir}"
      end

    end


    class TaskHandlerOptions

      def initialize(argv)
        @argv = argv
        @environment = Context::DEFAULT_ENV
        @daemon = false
        @log_file_path = nil
        @pid_file_path = nil
        @working_dir = Dir.getwd
        @task_id = nil
        @force = false
        @noop = false
        @rest_arguments = nil

        @opts = opts = OptionParser.new("Usage: #{$0} CONFIG_PATH")
        opts.on('-e', '--environment=NAME', "Sets execution environment [default: #{@environment}]") {|env|
          @environment = env
        }
        opts.on('--daemon', 'Becomes daemon in server mode.') {
          @daemon = true
        }
        opts.on('--log-file=PATH', 'Log file path') {|path|
          @log_file_path = path
        }
        opts.on('--pid-file=PATH', 'Creates PID file.') {|path|
          @pid_file_path = path
        }
        opts.on('--working-dir=PATH', "Loader working directory. [default: #{@working_dir}]") {|path|
          @working_dir = path
        }
        opts.on('--task-id=ID', 'Execute oneshot load task (implicitly disables daemon mode).') {|task_id|
          @task_id = task_id
        }
        opts.on('--force', 'Disables loaded check.') {|path|
          @force = true
        }
        opts.on('--noop', 'Does not execute tasks.') {
          @noop = true
        }
        opts.on('--help', 'Prints this message and quit.') {
          puts opts.help
          exit 0
        }
        opts.on('--version', 'Prints version and quit.') {
          puts "#{File.basename($0)} version #{VERSION}"
          exit 0
        }
      end

      def usage
        @opts.help
      end

      def parse
        @opts.parse!(@argv)
        @rest_arguments = @argv.dup
      rescue OptionParser::ParseError => err
        raise OptionError, err.message
      end

      attr_reader :rest_arguments

      attr_reader :environment

      def daemon?
        @daemon
      end

      attr_reader :log_file_path
      attr_reader :pid_file_path
      attr_reader :working_dir

      attr_reader :task_id

      def force?
        @force
      end

      def noop?
        @noop
      end

    end

  end

end

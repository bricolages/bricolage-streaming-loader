require 'bricolage/exception'
require 'bricolage/version'
require 'bricolage/sqsdatasource'
require 'bricolage/streamingload/event'
require 'bricolage/streamingload/objectbuffer'
require 'bricolage/streamingload/urlpatterns'
require 'aws-sdk'
require 'yaml'
require 'optparse'
require 'fileutils'

module Bricolage

  module StreamingLoad

    class Dispatcher

      def Dispatcher.main
        opts = DispatcherOptions.new(ARGV)
        opts.parse
        unless opts.rest_arguments.size == 1
          $stderr.puts opts.usage
          exit 1
        end
        config_path, * = opts.rest_arguments
        set_log_path opts.log_file_path if opts.log_file_path

        config = YAML.load(File.read(config_path))
        ctx = Context.for_application('.', environment: opts.environment)
        event_queue = ctx.get_data_source('sqs', config.fetch('event-queue-ds'))
        task_queue = ctx.get_data_source('sqs', config.fetch('task-queue-ds'))

        object_buffer = ObjectBuffer.new(
          control_data_source: ctx.get_data_source('sql', config.fetch('ctl-postgres-ds')),
          logger: ctx.logger
        )

        url_patterns = URLPatterns.for_config(config.fetch('url_patterns'))

        dispatcher = Dispatcher.new(
          event_queue: event_queue,
          task_queue: task_queue,
          object_buffer: object_buffer,
          url_patterns: url_patterns,
          dispatch_interval: 60,
          logger: ctx.logger
        )

        Process.daemon(true) if opts.daemon?
        create_pid_file opts.pid_file_path if opts.pid_file_path
        dispatcher.set_dispatch_timer
        dispatcher.event_loop
      end

      def Dispatcher.set_log_path(path)
        FileUtils.mkdir_p File.dirname(path)
        # make readable for retrieve_last_match_from_stderr
        File.open(path, 'w+') {|f|
          $stdout.reopen f
          $stderr.reopen f
        }
      end

      def Dispatcher.create_pid_file(path)
        File.open(path, 'w') {|f|
          f.puts $$
        }
      rescue
        # ignore
      end

      def initialize(event_queue:, task_queue:, object_buffer:, url_patterns:, dispatch_interval:, logger:)
        @event_queue = event_queue
        @task_queue = task_queue
        @object_buffer = object_buffer
        @url_patterns = url_patterns
        @dispatch_interval = dispatch_interval
        @dispatch_message_id = nil
        @logger = logger
      end

      def event_loop
        @event_queue.main_handler_loop(handlers: self, message_class: Event)
      end

      def handle_shutdown(e)
        @event_queue.initiate_terminate
        @event_queue.delete_message(e)
      end

      def handle_data(e)
        unless e.created?
          @event_queue.delete_message(e)
          return
        end
        obj = e.loadable_object(@url_patterns)
        @object_buffer.put(obj)
        @event_queue.delete_message(e)
      end

      def handle_dispatch(e)
        if @dispatch_message_id == e.message_id
          tasks = @object_buffer.flush_tasks
          tasks.each {|task| @task_queue.put task }
          set_dispatch_timer
        end
        @event_queue.delete_message(e)
      end

      def set_dispatch_timer
        resp = @event_queue.send_message DispatchEvent.create(delay_seconds: @dispatch_interval)
        @dispatch_message_id = resp.message_id
      end

      def delete_events(events)
        events.each do |e|
          @event_queue.delete_message(e)
        end
      end

    end


    class DispatcherOptions

      def initialize(argv)
        @argv = argv
        @daemon = false
        @log_file_path = nil
        @pid_file_path = nil
        @rest_arguments = nil

        @opts = opts = OptionParser.new("Usage: #{$0} CONFIG_PATH")
        opts.on('--task-id=id', 'Execute oneshot load task (implicitly disables daemon mode).') {|task_id|
          @task_id = task_id
        }
        opts.on('-e', '--environment=NAME', "Sets execution environment [default: #{Context::DEFAULT_ENV}]") {|env|
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

      attr_reader :rest_arguments, :environment, :log_file_path

      def daemon?
        @daemon
      end

      attr_reader :pid_file_path

    end

  end   # module StreamingLoad

end   # module Bricolage

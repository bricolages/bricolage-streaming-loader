require 'stringio'

module Bricolage

  module StreamingLoad

    class LoadTaskLogger

      def initialize(s3ds)
        @s3ds = s3ds
      end

      def log(task)
        csv = content(task)
        key = key(task)
        @logger.info "s3: put: #{@s3ds.url(key)}"
        @s3ds.object(key).put(body: csv)
      end

      def key(task)
        now = Time.now
        "task/#{now.strftime('%Y/%m/%d')}/task-#{task.id}.csv"
      end

      def content(task)
        buf = StringIO.new
        tasks.chunks.each do |chunk|
          buf.puts %Q("#{task.id}","#{chunk.id}","#{chunk.url}")
        end
        buf.string
      end

    end

  end

end

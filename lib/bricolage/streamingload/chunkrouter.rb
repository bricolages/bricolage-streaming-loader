require 'bricolage/streamingload/incomingchunk'

module Bricolage

  module StreamingLoad

    class ChunkRoutingFailed < StandardError; end


    class ChunkRouter

      def ChunkRouter.for_config(configs)
        new(configs.map {|c|
          Route.new(url: c.fetch('url'), schema: c.fetch('schema'), table: c.fetch('table'))
        })
      end

      def initialize(routes)
        @routes = routes
      end

      def route(msg)
        @routes.each do |route|
          stream_name = route.match(msg.url)
          return IncomingChunk.new(msg, stream_name) if stream_name
        end
        raise ChunkRoutingFailed, "could not detect stream name: #{url.inspect}"
      end

      class Route
        def initialize(url:, schema:, table:)
          @url_pattern = /\A#{url}\z/
          @schema = schema
          @table = table
        end

        def match(url)
          m = @url_pattern.match(url) or return nil
          c1 = get_component(m, @schema)
          c2 = get_component(m, @table)
          "#{c1}.#{c2}"
        end

        def get_component(m, label)
          if /\A%/ =~ label
            m[label[1..-1]]
          else
            label
          end
        end
      end

    end

  end   # module StreamingLoad

end   # module Bricolage

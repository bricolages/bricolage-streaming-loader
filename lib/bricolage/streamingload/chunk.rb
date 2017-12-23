module Bricolage

  module StreamingLoad

    class Chunk
      def initialize(id:, url:, size: nil)
        @id = id
        @url = url
        @size = size
      end

      attr_reader :id
      attr_reader :url
      attr_reader :size
    end

  end

end

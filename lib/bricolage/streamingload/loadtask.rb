module Bricolage

  module StreamingLoad

    class LoadTask
      def initialize(id:, chunks: [])
        @id = id
        @chunks = chunks
      end

      attr_reader :id
      attr_reader :chunks
    end

  end

end

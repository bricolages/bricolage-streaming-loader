require 'forwardable'

module Bricolage

  module StreamingLoad

    # a Chunk which is not saved yet (received from SQS)
    class IncomingChunk

      extend Forwardable

      def initialize(message, stream_name)
        @chunk = message.chunk
        @message = message
        @stream_name = stream_name
      end

      def_delegator '@chunk', :id
      def_delegator '@chunk', :url
      def_delegator '@chunk', :size

      def_delegator '@message', :message_id
      def_delegator '@message', :receipt_handle

      def event_time
        @message.time
      end

      attr_reader :stream_name

    end

  end

end

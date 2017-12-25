require 'test/unit'
require 'bricolage/streamingload/dispatchermessage'

module Bricolage::StreamingLoad

  class TestDispatcherMessage < Test::Unit::TestCase

    def new_s3event(message_id: nil, receipt_handle: nil, name: nil, time: nil, source: nil, region: nil, bucket: nil, key: nil, size: nil)
      S3ObjectDispatcherMessage.new(
        message_id: message_id,
        receipt_handle: receipt_handle,
        name: name,
        time: time,
        source: source,
        region: region,
        bucket: bucket,
        key: key,
        size: size
      )
    end

    test "#created?" do
      e = new_s3event(name: "ObjectCreated:Put")
      assert_true e.created_event?
      e = new_s3event(name: "ObjectCreated:Copy")
      assert_false e.created_event?
    end

  end

end

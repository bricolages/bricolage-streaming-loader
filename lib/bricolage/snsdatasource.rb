require 'bricolage/datasource'
require 'aws-sdk'
require 'json'
require 'time'

module Bricolage

  class SNSTopicDataSource < DataSource

    declare_type 'sns'

    def initialize(region: 'ap-northeast-1', topic_arn:, access_key_id:, secret_access_key:)
      @region = region
      @topic_arn = topic_arn
      @access_key_id = access_key_id
      @secret_access_key = secret_access_key
      @client = Aws::SNS::Client.new(region: region, access_key_id: access_key_id, secret_access_key: secret_access_key)
      @topic = Aws::SNS::Topic.new(topic_arn, client: @client)
    end

    attr_reader :region
    attr_reader :client, :topic

    def publish(message)
      @topic.publish(build_message(message))
    end

    alias write publish

    def close
      # do nothing
    end

    def build_message(message)
      {message: message}
    end

  end # SNSDataSource

end   # module Bricolage

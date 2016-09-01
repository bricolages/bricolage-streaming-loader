require 'bricolage/logger'
require 'logger'
require 'forwardable'

module Bricolage
  module StreamingLoad
    class AlertingLogger
      extend Forwardable

      def initialize(logger:, sns_datasource:, alert_level: 'warn')
        @logger = logger
        @alerter = Bricolage::Logger.new(device: sns_datasource)
        @alerter.level = ::Logger.const_get(alert_level.upcase)
      end

      def_delegators '@logger', :level, :level=, :debug?, :info?, :warn?, :error?, :fatal?, :unknown?

      %w[log debug info warn error fatal unknown].each do |m|
        define_method(m) do |*args|
          @logger.__send__(m, *args)
          begin
            @alerter.__send__(m, *args)
          rescue Exception => err
            @logger.error "could not send alert: #{err.message}"
          end
        end
      end

      def exception(ex)
        @logger.exception(ex)
        begin
          @alerter.error(ex.message)
        rescue Exception => err
          @logger.error "could not send alert: #{err.message}"
        end
      end

    end
  end
end

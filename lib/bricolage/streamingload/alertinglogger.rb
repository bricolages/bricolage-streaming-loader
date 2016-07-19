module Bricolage
  module StreamingLoad
    class AlertingLogger
      extend Forwardable

      def initialize(logger: , sns_datasource: , alert_level: 'warn')
        @logger = logger
        @sns_logger = Bricolage::Logger.new(device: sns_datasource)
        @sns_logger.level = Kernel.const_get("Logger").const_get(alert_level.upcase)
      end

      %w(log debug info warn error fatal unknown).each do |m|
        define_method(m) do |*args|
          [@logger, @sns_logger].map {|t| t.send(m, *args) }
        end
      end
    end
  end
end

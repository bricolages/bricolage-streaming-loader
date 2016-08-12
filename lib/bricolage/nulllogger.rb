require 'logger'

module Bricolage
  # FIXME: should be defined in the Bricolage package
  class NullLogger
    def debug(*args) end
    def debug?() false end
    def info(*args) end
    def info?() false end
    def warn(*args) end
    def warn?() false end
    def error(*args) end
    def error?() false end
    def exception(*args) end
    def with_elapsed_time(*args) yield end
    def elapsed_time(*args) yield end
    def level() Logger::ERROR end
    def level=(l) l end
  end
end

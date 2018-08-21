require_relative 'lib/bricolage/streamingload/version'

Gem::Specification.new do |s|
  s.platform = Gem::Platform::RUBY
  s.name = 'bricolage-streamingload'
  s.version = Bricolage::StreamingLoad::VERSION
  s.summary = 'Bricolage Streaming Load Daemon'
  s.description = 'Bricolage Streaming Load Daemon loads S3 data files to Redshift continuously.'
  s.license = 'MIT'

  s.author = ['Minero Aoki', 'Shimpei Kodama']
  s.email = ['aamine@loveruby.net']
  s.homepage = 'https://github.com/aamine/bricolage-streamingload'

  s.files = `git ls-files -z`.split("\x0").reject {|f| f.match(%r{^(test|spec|features)/}) }
  s.executables = s.files.grep(%r{bin/}).map {|path| File.basename(path) }
  s.require_path = 'lib'

  s.required_ruby_version = '>= 2.3.0'
  s.add_dependency 'bricolage', '>= 5.29.2'
  s.add_dependency 'pg', '~> 0.18.0'
  s.add_dependency 'aws-sdk-s3', '~> 1.8'
  s.add_dependency 'aws-sdk-sqs', '~> 1.3'
  s.add_development_dependency 'rake'
  s.add_development_dependency 'test-unit'
end

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

  s.executables = Dir.entries('bin').select {|ent| File.file?("bin/#{ent}") }
  s.files = Dir.glob(%w[README.md LICENSE bin/* lib/**/*.rb test/**/*])
  s.require_path = 'lib'

  s.required_ruby_version = '>= 2.1.0'
  s.add_dependency 'bricolage', '~> 5.17.2'
  s.add_dependency 'pg', '~> 0.18.0'
  s.add_dependency 'aws-sdk', '~> 2.6.2'
  s.add_development_dependency 'rake'
  s.add_development_dependency 'test-unit'
  s.add_development_dependency 'pry'
end

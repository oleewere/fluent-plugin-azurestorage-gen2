# encoding: utf-8
$:.push File.expand_path('../lib', __FILE__)

Gem::Specification.new do |gem|
  gem.name = "fluent-plugin-azurestorage-gen2"
  gem.description = "Azure Storage output plugin for Fluentd event collector"
  gem.license = "MIT License"
  gem.homepage = "https://github.com/oleewere/fluent-plugin-azurestorage-gen2"
  gem.summary = gem.description
  gem.version = File.read("VERSION").strip
  gem.authors = ["Oliver Szabo"]
  gem.email = ["oleewere@gmail.com"]
  #gem.platform    = Gem::Platform::RUBY
  gem.files = `git ls-files`.split("\n")
  gem.test_files = `git ls-files -- {test,spec,features}/*`.split("\n")
  gem.executables = `git ls-files -- bin/*`.split("\n").map {|f| File.basename(f)}
  gem.require_paths = ['lib']

  gem.add_runtime_dependency 'fluentd', ['>= 1.0', '< 2']
  gem.add_runtime_dependency 'uuidtools', '~> 2.1', '>= 2.1.5'
  gem.add_runtime_dependency 'typhoeus', '~> 1.0', '>= 1.0.1'
  gem.add_runtime_dependency 'json', '~> 2.1', '>= 2.1.0'
  gem.add_runtime_dependency "yajl-ruby", '~> 1.4'
  gem.add_runtime_dependency 'concurrent-ruby', '~> 1.1', '>= 1.1.5'
  gem.add_development_dependency 'rake', '~> 12.3', '>= 12.3.1'
  gem.add_development_dependency 'test-unit', '~> 3.3', '>= 3.3.3'
  gem.add_development_dependency 'test-unit-rr', '~> 1.0', '>= 1.0.5'
end

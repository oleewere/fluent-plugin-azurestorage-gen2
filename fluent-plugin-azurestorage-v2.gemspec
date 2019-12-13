# encoding: utf-8
$:.push File.expand_path('../lib', __FILE__)

require 'open3'

Gem::Specification.new do |gem|

  arg_file = File.expand_path("../GEMSPEC_ARGS", __FILE__)
  unless File.file?(arg_file)
    raise 'Missing GEMSPEC_ARGS, please build the gem by "make build".'
  end

  args = []
  File.open(arg_file).each_line do |line|
    line = line.chomp
    unless line.empty?
      args.push(line)
    end
  end

  version_pattern = '^[0-9]+\.[0-9]+\.[0-9]+(\.pre)?$'
  version = args[0]
  unless version.=~ /#{version_pattern}/
    raise StandardError, "Invalid version. Should be {number}.{number}.{number}(.pre)?"
  end

  latest_rel = Open3.capture2('git describe --tags --match "v[0-9]*.[0-9]*.[0-9]*" --always | cut -d "-" -f 1')[0].strip
  if latest_rel.=~ /v#{version_pattern}/ and version == latest_rel[1..latest_rel.length]
    raise StandardError, "Cannot build in released version: #{version}"
  end

  gem.name = "fluent-plugin-azurestorage-v2"
  gem.description = "Azure Storage output plugin for Fluentd event collector"
  gem.license = "Apache-2.0"
  gem.homepage = "https://github.com/oleewere/fluent-plugin-azurestorage-v2"
  gem.summary = gem.description
  gem.version = version
  gem.authors = ["Oliver Szabo"]
  gem.email = ["oleewere@gmail.com"]
  #gem.platform    = Gem::Platform::RUBY
  gem.files = `git ls-files`.split("\n")
  gem.test_files = `git ls-files -- {test,spec,features}/*`.split("\n")
  gem.executables = `git ls-files -- bin/*`.split("\n").map {|f| File.basename(f)}
  gem.require_paths = ['lib']

  gem.add_runtime_dependency 'fluentd', ['>= 1.0', '< 2']
  gem.add_runtime_dependency 'azure-storage-common', '~> 1.1', '>= 1.1.0'
  gem.add_runtime_dependency 'azure-storage-blob', '~> 1.1', '>= 1.1.0'
  gem.add_runtime_dependency 'uuidtools', '~> 2.1', '>= 2.1.5'
  gem.add_runtime_dependency 'typhoeus', '~> 1.0', '>= 1.0.1'
  gem.add_runtime_dependency 'json', '~> 2.1', '>= 2.1.0'
  gem.add_runtime_dependency "yajl-ruby", '~> 1.4'
  gem.add_development_dependency 'rake', '~> 12.3', '>= 12.3.1'
  gem.add_development_dependency 'test-unit', '~> 3.3', '>= 3.3.3'
  gem.add_development_dependency 'test-unit-rr', '~> 1.0', '>= 1.0.5'
end

gem:
	rm -f fluent-plugin-azurestorage-gen2*.gem
	gem build fluent-plugin-azurestorage-gen2.gemspec

install: gem
	gem install fluent-plugin-azurestorage-gen2*.gem

push: gem
	gem push fluent-plugin-azurestorage-gen2*.gem
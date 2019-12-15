gem:
	rm -f fluent-plugin-azurestorage-gen2*.gem
	gem build fluent-plugin-azurestorage-gen2.gemspec

install: gem
	gem install fluent-plugin-azurestorage-gen2*.gem

push: gem
	gem push fluent-plugin-azurestorage-gen2*.gem

tag-and-branch:
	git tag "v$$(cat VERSION)" $(RELEASE_COMMIT)
	git checkout -b "release/$$(cat VERSION)" $(RELEASE_COMMIT)
	git push origin "v$$(cat VERSION)"
	git push -u origin "release/$$(cat VERSION)"
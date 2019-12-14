unit-test:
	bundler install --path ~/.gem
	#ruby -Ilib:test test/*.rb

tag-and-branch:
	git tag "v$$(cat VERSION)" $(RELEASE_COMMIT)
	git checkout -b "release/$$(cat VERSION)" $(RELEASE_COMMIT)
	git push origin "v$$(cat VERSION)"
	git push -u origin "release/$$(cat VERSION)"

pre-args:
	(head -1 VERSION | tr -d '\n'; echo '.pre') > GEMSPEC_ARGS
	echo $$(git rev-parse HEAD) >> GEMSPEC_ARGS

stable-args:
	(head -1 VERSION | tr -d '\n'; echo '') > GEMSPEC_ARGS

gem:
	rm -f fluent-plugin-azurestorage-gen2*.gem
	gem build fluent-plugin-azurestorage-gen2.gemspec

push-gem:
	gem push fluent-plugin-azurestorage-gen2-$$(head -1 GEMSPEC_ARGS).gem

build: pre-args unit-test gem push-gem

local-build: pre-args unit-test gem

local-release: stable-args gem

release: stable-args unit-test tag-and-branch gem push-gem

#!/bin/sh

#docker build -t oleewere/fluent-plugin-azurestorage-gen2:latest .

docker run -it --rm \
  -v $(pwd)/example:/fluentd/etc \
  -v $(pwd)/test-folder:/fluentd/test \
  -v $(pwd)/lib/fluent/plugin/out_azurestorage_gen2.rb:/fluentd/plugins/out_azurestorage_gen2.rb \
  oleewere/fluent:latest

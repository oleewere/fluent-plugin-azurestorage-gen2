require 'fluent/test'
require 'fluent/test/driver/output'
require 'fluent/test/helpers'
require 'fluent/plugin/out_azurestorage_gen2'

require 'test/unit/rr'
require 'zlib'
require 'fileutils'

include Fluent::Test::Helpers

class AzureStorageGen2OutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    azure_storage_account test_storage_account
    azure_storage_access_key dGVzdF9zdG9yYWdlX2FjY2Vzc19rZXk=
    azure_container test_container
    path log
    utc
    buffer_type memory
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::AzureStorageGen2Output) do
      # for testing.
      def contents
        @emit_streams
      end

      def write(chunk)
        @emit_streams = []
        event = chunk.read
        @emit_streams << event
      end

      private
      def ensure_container
      end

    end.configure(conf)
  end

  def test_configure
    # TODO write tests
    d = create_driver
    assert_equal 'test_storage_account', d.instance.azure_storage_account
    assert_equal 'dGVzdF9zdG9yYWdlX2FjY2Vzc19rZXk=', d.instance.azure_storage_access_key
    assert_equal 'test_container', d.instance.azure_container
    assert_equal 'log', d.instance.path
  end
end

require 'net/http'
require 'json'
require 'tempfile'
require 'time'
require 'typhoeus'
require 'fluent/plugin/output'
require 'zlib'

module Fluent::Plugin
    class AzureStorageGen2Output < Fluent::Plugin::Output
      Fluent::Plugin.register_output('azurestorage_gen2', self)

        helpers :compat_parameters, :formatter, :inject

        def initialize
            super
            @compressor = nil
        end
      
        config_param :path, :string, :default => ""
        config_param :azure_storage_account, :string, :default => nil
        config_param :azure_storage_access_key, :string, :default => nil, :secret => true
        config_param :azure_instance_msi, :string, :default => nil
        config_param :azure_oauth_refresh_interval, :integer, :default => 60 * 60 # one hour
        config_param :azure_container, :string, :default => nil
        config_param :azure_storage_type, :string, :default => "blob"
        config_param :azure_object_key_format, :string, :default => "%{path}%{time_slice}_%{index}.%{file_extension}"
        config_param :store_as, :string, :default => "gzip"
        config_param :auto_create_container, :bool, :default => false
        config_param :format, :string, :default => "out_file"
        config_param :command_parameter, :string, :default => nil
        config_param :time_slice_format, :string, :default => '%Y%m%d'

        DEFAULT_FORMAT_TYPE = "out_file"
        URL_DOMAIN_SUFFIX = '.dfs.core.windows.net'
        ACCESS_TOKEN_API_VERSION = "2018-02-01"
        ABFS_API_VERSION = "2018-11-09"
        AZURE_BLOCK_SIZE_LIMIT = 4 * 1024 * 1024 - 1

        config_section :format do
            config_set_default :@type, DEFAULT_FORMAT_TYPE
        end
  
        config_section :buffer do
            config_set_default :chunk_keys, ['time']
            config_set_default :timekey, (60 * 60 * 24)
        end

        def configure(conf)
            compat_parameters_convert(conf, :buffer, :formatter, :inject)
            super

            @formatter = formatter_create
      
            if @localtime
              @path_slicer = Proc.new {|path|
                Time.now.strftime(path)
              }
            else
              @path_slicer = Proc.new {|path|
                Time.now.utc.strftime(path)
              }
            end
      
            if @azure_container.nil?
              raise Fluent::ConfigError, "azure_container is needed"
            end
      
            @storage_type = case @azure_storage_type
                              when 'tables'
                                raise NotImplementedError
                              when 'queues'
                                raise NotImplementedError
                              else
                                'blob'
                            end
            @azure_storage_path = ''
            @last_azure_storage_path = ''
            @current_index = 0
        end

        def multi_workers_ready?
            true
        end

        def start
            setup_access_token
            ensure_container
            super
        end

        def write(chunk)
            metadata = chunk.metadata
            
            #tmp = Tempfile.new("azure-")
            #begin
            #    tmp.close
            #    generate_log_name(metadata, @current_index)
            #    if @last_azure_storage_path != @azure_storage_path
            #        @current_index = 0
            #        generate_log_name(metadata, @current_index)
            #    end
            #    content = File.open(tmp.path, 'rb') { |file| file.read }
            #    raw_data = raw_data.chomp
            #    log.debug "Content: #{content}"
            #    upload_blob(content, metadata)
            #    @last_azure_storage_path = @azure_storage_path
            #ensure
            #    tmp.close(true) rescue nil
            #    tmp.unlink
            #end
            raw_data=''
            generate_log_name(metadata, @current_index)
            if @last_azure_storage_path != @azure_storage_path
                @current_index = 0
                generate_log_name(metadata, @current_index)
            end
            chunk.each do |emit_time, record|
                line = record["message"].chomp
                raw_data << "#{line}\n"
            end
            raw_data = raw_data.chomp
            unless raw_data.empty?
                upload_blob(raw_data, metadata)
            end
            @last_azure_storage_path = @azure_storage_path

        end

        private
        def upload_blob(content, metadata)
            log.debug "azurestorage_gen2:  Uploading blob: #{@azure_storage_path}"
            existing_content_length = get_blob_properties(@azure_storage_path)
            if existing_content_length == 0
                create_blob(@azure_storage_path)
            end
            append_blob(content, metadata, existing_content_length)
        end

        def format(tag, time, record)
            r = inject_values_to_record(tag, time, record)
            @formatter.format(tag, time, r)
        end

        private
        def generate_log_name(metadata, index)
            time_slice = if metadata.timekey.nil?
                       ''.freeze
                     else
                       Time.at(metadata.timekey).utc.strftime(@time_slice_format)
                     end
            path = @path_slicer.call(@path)
            values_for_object_key = {
                "%{path}" => path,
                "%{time_slice}" => time_slice,
                "%{index}" => index
            }
            storage_path = @azure_object_key_format.gsub(%r(%{[^}]+}), values_for_object_key)
            extracted_path = extract_placeholders(storage_path, metadata)
            extracted_path = "/" + extracted_path unless extracted_path.start_with?("/")
            @azure_storage_path = extracted_path
        end

        def setup_access_token
            @get_token_lock = Concurrent::ReadWriteLock.new
            acquire_access_token
            if @azure_oauth_refresh_interval > 0
                log.info("azurestorage_gen2: Start getting access token every #{@azure_oauth_refresh_interval} seconds.")
                @get_token_task = Concurrent::TimerTask.new(
                    execution_interval: @azure_oauth_refresh_interval) {
                    begin
                        acquire_access_token
                    rescue Exception => e
                        log.warn("#{e.message}, continue with previous credentials.")
                    end
                }
                @get_token_task.execute
            end
        end

        # Referenced from azure doc.
        # https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/tutorial-linux-vm-access-storage#get-an-access-token-and-use-it-to-call-azure-storage
        def acquire_access_token
            params = { :"api-version" => ACCESS_TOKEN_API_VERSION, :resource => "https://storage.azure.com/" }
            unless @azure_instance_msi.nil?
                params[:msi_res_id] = @azure_instance_msi
            end
            request = Typhoeus::Request.new("http://169.254.169.254/metadata/identity/oauth2/token", params: params, headers: { Metadata: "true"})
            request.on_complete do |response|
                if response.success?
                  data = JSON.parse(response.body)
                  log.debug "azurestorage_gen2: Token response: #{data}"
                  @azure_access_token = data["access_token"]
                else
                    raise Fluent::UnrecoverableError, "Failed to acquire access token. #{response.code}: #{response.body}"
                end
            end
            request.run
        end

        private
        def ensure_container
            headers = {:"x-ms-version" =>  ABFS_API_VERSION, :"Authorization" => "Bearer #{@azure_access_token}",:"Content-Length" => "0"}
            params = {:resource => "filesystem" }
            request = Typhoeus::Request.new("https://#{azure_storage_account}#{URL_DOMAIN_SUFFIX}/#{@azure_container}", :method => :head, :params => params, :headers=> headers)
            request.on_complete do |response|
                if response.success?
                  log.info "azurestorage_gen2: Container '#{@azure_container}' exists."
                elsif response.timed_out?
                    raise Fluent::UnrecoverableError,  "Get container '#{@azure_container}' request timed out."
                elsif response.code == 404
                    log.info "azurestorage_gen2: Container '#{@azure_container}' does not exist. Creating it if needed..."
                    if @auto_create_container
                        create_container
                    else
                        raise Fluent::ConfigError, "The specified container does not exist: container = #{@azure_container}"
                    end
                else
                    raise Fluent::UnrecoverableError, "Get container request failed - code: #{response.code}, body: #{response.body}"
                end
            end
            request.run
        end

        private
        def create_container
            headers = {:"x-ms-version" =>  ABFS_API_VERSION, :"Authorization" => "Bearer #{@azure_access_token}",:"Content-Length" => "0"}
            params = {:resource => "filesystem" }
            request = Typhoeus::Request.new("https://#{azure_storage_account}#{URL_DOMAIN_SUFFIX}/#{@azure_container}", :method => :put, :params => params, :headers=> headers)
            request.on_complete do |response|
                if response.success?
                    log.debug "azurestorage_gen2: Container '#{@azure_container}' created, response code: #{response.code}"
                elsif response.timed_out?
                    raise Fluent::UnrecoverableError,  "Creating container '#{@azure_container}' request timed out."
                else
                    raise Fluent::UnrecoverableError, "Creating container request failed - code: #{response.code}, body: #{response.body}"
                end
            end
            request.run
        end

        private
        def create_blob(blob_path)
            headers = {:"x-ms-version" =>  ABFS_API_VERSION, :"Authorization" => "Bearer #{@azure_access_token}",:"Content-Length" => "0", :"Content-Type" => "application/json"}
            params = {:resource => "file", :recursive => "false"}
            request = Typhoeus::Request.new("https://#{azure_storage_account}#{URL_DOMAIN_SUFFIX}/#{@azure_container}/testprefix#{blob_path}", :method => :put, :params => params, :headers=> headers)
            request.on_complete do |response|
                if response.success?
                    log.debug "azurestorage_gen2: Blob '#{blob_path}' has been created, response code: #{response.code}"
                elsif response.timed_out?
                    raise Fluent::UnrecoverableError,  "Creating blob '#{@blob_path}' request timed out."
                elsif response.code == 409
                    log.debug "azurestorage_gen2: Blob already exists: #{blob_path}"
                else
                    raise Fluent::UnrecoverableError, "Creating blob '#{blob_path}' request failed - code: #{response.code}, body: #{response.body}"
                end
            end
            request.run
        end

        private
        def append_blob_block(blob_path, content, position)
            log.debug "azurestorage_gen2: append_blob_block.start: Append blob ('#{blob_path}') called with position #{position}"
            headers = {:"x-ms-version" =>  ABFS_API_VERSION, :"x-ms-content-type" => "text/plain", :"Authorization" => "Bearer #{@azure_access_token}"}
            params = {:action => "append", :position => "#{position}"}
            request = Typhoeus::Request.new("https://#{azure_storage_account}#{URL_DOMAIN_SUFFIX}/#{@azure_container}/testprefix#{blob_path}", :method => :patch, :body => content, :params => params, :headers=> headers)
            request.on_complete do |response|
                if response.success?
                    log.debug "azurestorage_gen2: Blob '#{blob_path}' has been appended, response code: #{response.code}"
                elsif response.timed_out?
                    raise Fluent::UnrecoverableError,  "Appending blob #{@blob_path}' request timed out."
                elsif response.code == 404
                    raise AppendBlobResponseError.new("Blob '#{blob_path}' has not found. Error code: #{response.code}", 404)
                elsif response.code == 409
                    raise AppendBlobResponseError.new("Blob '#{blob_path}' has conflict. Error code: #{response.code}", 409)
                else
                    raise Fluent::UnrecoverableError, "Appending blob '#{blob_path}' request failed - code: #{response.code}, body: #{response.body}"
                end
            end
            request.run
        end

        private
        def flush(blob_path, position)
            log.debug "azurestorage_gen2: flush_blob.start: Flush blob ('#{blob_path}') called with position #{position}"
            headers = {:"x-ms-version" =>  ABFS_API_VERSION, :"Authorization" => "Bearer #{@azure_access_token}",:"Content-Length" => "0"}
            params = {:action => "flush", :position => "#{position}"}
            request = Typhoeus::Request.new("https://#{azure_storage_account}#{URL_DOMAIN_SUFFIX}/#{@azure_container}/testprefix#{blob_path}", :method => :patch, :params => params, :headers=> headers)
            request.on_complete do |response|
                if response.success?
                    log.debug "azurestorage_gen2: Blob '#{blob_path}' flush was successful, response code: #{response.code}"
                elsif response.timed_out?
                    raise Fluent::UnrecoverableError,  "Bloub '#{@blob_path}' flush request timed out."
                else
                    raise Fluent::UnrecoverableError, "Blob flush request failed - code: #{response.code}, body: #{response.body}"
                end
            end
            request.run
        end

        private
        def get_blob_properties(blob_path)
            headers = {:"x-ms-version" =>  ABFS_API_VERSION, :"Authorization" => "Bearer #{@azure_access_token}",:"Content-Length" => "0"}
            params = {}
            content_length = -1
            request = Typhoeus::Request.new("https://#{azure_storage_account}#{URL_DOMAIN_SUFFIX}/#{@azure_container}/testprefix#{blob_path}", :method => :head, :params => params, :headers=> headers)
            request.on_complete do |response|
                if response.success?
                  log.debug "azurestorage_gen2: Get blob properties for '#{blob_path}', response headers: #{response.headers}"
                  content_length = response.headers['Content-Length'].to_i
                elsif response.timed_out?
                    raise Fluent::UnrecoverableError,  "Get blob properties '#{@blob_path}' request timed out."
                elsif response.code == 404
                    log.debug "azurestorage_gen2: Blob '#{@blob_path}' does not exist. Creating it if needed..."
                    content_length = 0
                else
                    raise Fluent::UnrecoverableError, "Get blob properties '#{@blob_path}' request failed - code: #{response.code}, body: #{response.body}"
                end
            end
            request.run
            content_length
        end

        private
        def append_blob(content, metadata, existing_content_length)
          position = 0
          log.debug "azurestorage_gen2: append_blob.start: Content size: #{content.length}"
          loop do
            begin
              size = [content.length - position, AZURE_BLOCK_SIZE_LIMIT].min
              log.debug "azurestorage_gen2: append_blob.chunk: content[#{position}..#{position + size}]"
              append_blob_block(@azure_storage_path, content[position..position + size], existing_content_length)

              position += size
              existing_content_length += size
              break if position >= content.length
            rescue AppendBlobResponseError => ex
              status_code = ex.status_code
  
              if status_code == 409 # exceeds azure block limit
                @current_index += 1
                old_azure_storage_path = @azure_storage_path
                generate_log_name(metadata, @current_index)
  
                # If index is not a part of format, rethrow exception.
                if old_azure_storage_path == @azure_storage_path
                  log.warn "azurestorage_gen2: append_blob: blocks limit reached, you need to use %{index} for the format."
                  raise
                end
                flush(old_azure_storage_path, existing_content_length)
  
                log.info "azurestorage_gen2: append_blob: blocks limit reached, creating new blob #{@azure_storage_path}."
                create_blob(@azure_storage_path)
              elsif status_code == 404 # blob not found
                log.debug "azurestorage_gen2: append_blob: #{@azure_storage_path} blob doesn't exist, creating new blob."
                create_blob(@azure_storage_path)
              else
                raise
              end
            end
          end
          flush(@azure_storage_path, existing_content_length)
          log.debug "azurestorage_gen2: append_blob.complete"
        end
    end

    class AppendBlobResponseError < StandardError
        attr_reader :status_code
        def initialize(message="Default message", status_code=0)
          @status_code = status_code
          super(message)
        end
    end
end
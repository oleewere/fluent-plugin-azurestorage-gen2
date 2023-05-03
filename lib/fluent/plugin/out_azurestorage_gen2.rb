require 'net/http'
require 'base64'
require 'openssl'
require 'json'
require 'tempfile'
require 'time'
require 'typhoeus'
require 'fluent/plugin/output'
require 'concurrent'
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
        config_param :azure_client_id, :string, :default => nil
        config_param :azure_object_id, :string, :default => nil
        config_param :azure_oauth_app_id, :string, :default => nil, :secret => true
        config_param :azure_oauth_secret, :string, :default => nil, :secret => true
        config_param :azure_oauth_tenant_id, :string, :default => nil
        config_param :azure_oauth_identity_authority, :string, :default => "https://login.microsoftonline.com"
        config_param :azure_oauth_use_azure_cli, :bool, :default => false
        config_param :azure_oauth_refresh_interval, :integer, :default => 60 * 60
        config_param :azure_container, :string, :default => nil
        config_param :azure_object_key_format, :string, :default => "%{path}%{time_slice}_%{index}.%{file_extension}"
        config_param :file_extension, :string, :default => "log"
        config_param :store_as, :string, :default => "none"
        config_param :auto_create_container, :bool, :default => false
        config_param :skip_container_check, :bool, :default => false
        config_param :failsafe_container_check, :bool, :default => false
        config_param :enable_retry, :bool, :default => false
        config_param :startup_fail_on_error, :bool, :default => true
        config_param :url_domain_suffix, :string, :default => '.dfs.core.windows.net'
        config_param :url_storage_resource, :string, :default => 'https://storage.azure.com/'
        config_param :format, :string, :default => "out_file"
        config_param :time_slice_format, :string, :default => '%Y%m%d'
        config_param :hex_random_length, :integer, default: 4
        config_param :command_parameter, :string, :default => nil
        config_param :proxy_url, :string, :default => nil
        config_param :proxy_username, :string, :default => nil
        config_param :proxy_password, :string, :default => nil, :secret => true
        config_param :write_only, :bool, :default => false
        config_param :upload_timestamp_format, :string, :default => '%H%M%S%L'
        config_param :http_timeout_seconds, :integer, :default => 120

        DEFAULT_FORMAT_TYPE = "out_file"
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

            if @store_as.nil? || @store_as == "none"
                log.info "azurestorage_gen2: Compression is disabled (store_as: #{@store_as})"
            else
                begin
                    @compressor = COMPRESSOR_REGISTRY.lookup(@store_as).new(:buffer_type => @buffer_type, :log => log)
                rescue => e
                    log.warn "#{@store_as} not found. Use 'text' instead"
                    @compressor = TextCompressor.new
                end
                @compressor.configure(conf)
            end

            @formatter = formatter_create

            if @azure_container.nil?
              raise Fluent::ConfigError, "azure_container is needed"
            end

            @azure_storage_path = ''
            @last_azure_storage_path = ''
            @current_index = 0

            if @store_as.nil? || @store_as == "none"
                @final_file_extension = @file_extension
            else
                @final_file_extension = @compressor.ext
            end
            @values_for_object_chunk = {}
        end

        def multi_workers_ready?
            true
        end

        def start
            setup_access_token
            if !@skip_container_check
                if @failsafe_container_check
                    begin
                        if @write_only && @auto_create_container
                            create_container
                        else
                            ensure_container
                        end
                    rescue Exception => e
                        log.warn("#{e.message}, container list/create failsafe is enabled. Continue without those operations.")
                    end
                else
                    if @write_only && @auto_create_container
                        create_container
                    else
                        ensure_container
                    end
                end
            end
            super
        end

        def format(tag, time, record)
            r = inject_values_to_record(tag, time, record)
            @formatter.format(tag, time, r)
        end

        def write(chunk)
            if @store_as.nil? || @store_as == "none"
                generate_log_name(chunk, @current_index)
                if @last_azure_storage_path != @azure_storage_path
                    @current_index = 0
                    generate_log_name(chunk, @current_index)
                end
                raw_data = chunk.read
                unless raw_data.empty?
                    log.debug "azurestorage_gen2: processing raw data", chunk_id: dump_unique_id_hex(chunk.unique_id)
                    upload_blob(raw_data, chunk)
                end
                chunk.close rescue nil
                @last_azure_storage_path = @azure_storage_path
            else
                tmp = Tempfile.new("azure-")
                tmp.binmode
                begin
                    @compressor.compress(chunk, tmp)
                    tmp.rewind
                    generate_log_name(chunk, @current_index)
                    if @last_azure_storage_path != @azure_storage_path
                        @current_index = 0
                        generate_log_name(chunk, @current_index)
                    end
                    log.debug "azurestorage_gen2: Start uploading temp file: #{tmp.path}"
                    content = File.open(tmp.path, 'rb') { |file| file.read }
                    upload_blob(content, chunk)
                    @last_azure_storage_path = @azure_storage_path
                ensure
                    tmp.close(true) rescue nil
                end
                @values_for_object_chunk.delete(chunk.unique_id)
            end

        end

        private
        def upload_blob(content, chunk)
            log.debug "azurestorage_gen2: Uploading blob: #{@azure_storage_path}"
            if @write_only
                create_blob(@azure_storage_path)
                append_blob(content, chunk, 0)
            else
                existing_content_length = get_blob_properties(@azure_storage_path)
                if existing_content_length == 0
                    create_blob(@azure_storage_path)
                end
                append_blob(content, chunk, existing_content_length)
            end
        end

        private
        def generate_log_name(chunk, index)
            metadata = chunk.metadata
            time_slice = if metadata.timekey.nil?
                       ''.freeze
                     else
                       Time.at(metadata.timekey).utc.strftime(@time_slice_format)
                     end
            if @localtime
                hms_slicer = Time.now.strftime("%H%M%S")
                upload_timestamp = Time.now.strftime(@upload_timestamp_format)
            else
                hms_slicer = Time.now.utc.strftime("%H%M%S")
                upload_timestamp = Time.now.utc.strftime(@upload_timestamp_format)
            end

            @values_for_object_chunk[chunk.unique_id] ||= {
                "%{hex_random}" => hex_random(chunk),
            }
            values_for_object_key_pre = {
                "%{path}" => @path,
                "%{index}" => index,
                "%{uuid_flush}" => uuid_random,
                "%{file_extension}" => @final_file_extension,
                "%{upload_timestamp}" => upload_timestamp,
            }
            values_for_object_key_post = {
                "%{date_slice}" => time_slice,
                "%{time_slice}" => time_slice,
                "%{hms_slice}" => hms_slicer,
            }.merge!(@values_for_object_chunk[chunk.unique_id])
            storage_path = @azure_object_key_format.gsub(%r(%{[^}]+})) do |matched_key|
                values_for_object_key_pre.fetch(matched_key, matched_key)
            end
            storage_path = extract_placeholders(storage_path, chunk)
            storage_path = storage_path.gsub(%r(%{[^}]+}), values_for_object_key_post)
            storage_path = "/" + storage_path unless storage_path.start_with?("/")
            @azure_storage_path = storage_path
        end

        def setup_access_token
            if @azure_storage_access_key.nil?
                @get_token_lock = Concurrent::ReadWriteLock.new
                if @startup_fail_on_error
                    acquire_access_token
                else
                    while true
                        begin
                            acquire_access_token
                            break
                        rescue Exception => e
                            log.warn("#{e.message}, acquired token failed, wait 20 seconds until next retry.")
                            sleep 20
                        end
                    end
                end
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
            else
                log.info "azurestorage_gen2: Access storage key is configured, MSI support is disabled."
            end
        end

        def acquire_access_token
            if !@azure_instance_msi.nil?
                acquire_access_token_msi
            elsif !@azure_oauth_app_id.nil? and !@azure_oauth_secret.nil? and !@azure_oauth_tenant_id.nil?
                acquire_access_token_oauth_app
            elsif @azure_oauth_use_azure_cli
                acquire_access_token_by_az
            else
                raise Fluent::UnrecoverableError, "Using MSI or 'az cli tool' or simple OAuth 2.0 based authentication parameters (azure_oauth_tenant_id, azure_oauth_app_id, azure_oauth_secret) are required."
            end
        end

        # Referenced from azure doc.
        # https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/tutorial-linux-vm-access-storage#get-an-access-token-and-use-it-to-call-azure-storage
        private
        def acquire_access_token_msi
            params = { :"api-version" => ACCESS_TOKEN_API_VERSION, :resource => "#{@url_storage_resource}" }
            unless @azure_instance_msi.nil?
                params[:msi_res_id] = @azure_instance_msi
            end
            unless @azure_client_id.nil?
                params[:client_id] = @azure_client_id
            end
            unless @azure_object_id.nil?
                params[:object_id] = @azure_object_id
            end
            req_opts = {
                :params => params,
                :headers => { Metadata: "true" },
                :timeout => @http_timeout_seconds
            }
            add_proxy_options(req_opts)
            request = Typhoeus::Request.new("http://169.254.169.254/metadata/identity/oauth2/token", req_opts)
            request.on_complete do |response|
                if response.success?
                  data = JSON.parse(response.body)
                  log.debug "azurestorage_gen2: Token response: #{data}"
                  @azure_access_token = data["access_token"].chomp
                else
                    raise Fluent::UnrecoverableError, "Failed to acquire access token. #{response.code}: #{response.body}"
                end
            end
            request.run
        end

        private
        def acquire_access_token_oauth_app
            params = { :"api-version" => ACCESS_TOKEN_API_VERSION, :resource => "#{@url_storage_resource}"}
            headers = {:"Content-Type" => "application/x-www-form-urlencoded"}
            content = "grant_type=client_credentials&client_id=#{@azure_oauth_app_id}&client_secret=#{@azure_oauth_secret}&resource=#{@url_storage_resource}"
            req_opts = {
                :params => params,
                :body => content, 
                :headers => headers,
                :timeout => @http_timeout_seconds
            }
            add_proxy_options(req_opts)
            request = Typhoeus::Request.new("#{@azure_oauth_identity_authority}/#{@azure_oauth_tenant_id}/oauth2/token", req_opts)
            request.on_complete do |response|
                if response.success?
                  data = JSON.parse(response.body)
                  log.debug "azurestorage_gen2: Token response: #{data}"
                  @azure_access_token = data["access_token"].chomp
                else
                    raise Fluent::UnrecoverableError, "Failed to acquire access token. #{response.code}: #{response.body}"
                end
            end
            request.run
        end

        private
        def acquire_access_token_by_az
            access_token=`az account get-access-token --resource #{@url_storage_resource} --query accessToken -o tsv`
            log.debug "azurestorage_gen2: Token response: #{access_token}"
            @azure_access_token = access_token.chomp
        end

        private
        def ensure_container
            datestamp = create_request_date
            headers = {:"x-ms-version" =>  ABFS_API_VERSION, :"x-ms-date" => datestamp,:"Content-Length" => "0"}
            params = {:resource => "filesystem" }
            auth_header = create_auth_header("head", datestamp, "#{@azure_container}", headers, params)
            headers[:Authorization] = auth_header
            req_opts = {
                :method => :head,
                :params => params,
                :headers => headers,
                :timeout => @http_timeout_seconds
            }
            add_proxy_options(req_opts)
            request = Typhoeus::Request.new("https://#{azure_storage_account}#{@url_domain_suffix}/#{@azure_container}", req_opts)
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
                    raise Fluent::UnrecoverableError, "Get container request failed - code: #{response.code}, headers: #{response.headers}"
                end
            end
            request.run
        end

        private
        def create_container
            datestamp = create_request_date
            headers = {:"x-ms-version" =>  ABFS_API_VERSION, :"x-ms-date" => datestamp, :"Content-Length" => "0"}
            params = {:resource => "filesystem" }
            auth_header = create_auth_header("put", datestamp, "#{@azure_container}", headers, params)
            headers[:Authorization] = auth_header
            req_opts = {
                :method => :put,
                :params => params,
                :headers => headers,
                :timeout => @http_timeout_seconds
            }
            add_proxy_options(req_opts)
            request = Typhoeus::Request.new("https://#{azure_storage_account}#{@url_domain_suffix}/#{@azure_container}", req_opts)
            request.on_complete do |response|
                if response.success?
                    log.debug "azurestorage_gen2: Container '#{@azure_container}' created, response code: #{response.code}"
                elsif response.timed_out?
                    raise Fluent::UnrecoverableError, "Creating container '#{@azure_container}' request timed out."
                else
                    raise Fluent::UnrecoverableError, "Creating container request failed - code: #{response.code}, body: #{response.body}, headers: #{response.headers}"
                end
            end
            request.run
        end

        private
        def create_blob(blob_path)
            datestamp = create_request_date
            headers = {:"x-ms-version" =>  ABFS_API_VERSION, :"x-ms-date" => datestamp,:"Content-Length" => "0", :"Content-Type" => "text/plain"}
            params = {:resource => "file", :recursive => "false"}
            auth_header = create_auth_header("put", datestamp, "#{@azure_container}#{blob_path}", headers, params)
            headers[:Authorization] = auth_header
            req_opts = {
                :method => :put,
                :params => params,
                :headers => headers,
                :timeout => @http_timeout_seconds
            }
            add_proxy_options(req_opts)
            request = Typhoeus::Request.new("https://#{azure_storage_account}#{@url_domain_suffix}/#{@azure_container}#{blob_path}", req_opts)
            request.on_complete do |response|
                if response.success?
                    log.debug "azurestorage_gen2: Blob '#{blob_path}' has been created, response code: #{response.code}"
                elsif response.timed_out?
                    raise_error  "Creating blob '#{blob_path}' request timed out."
                elsif response.code == 409
                    log.debug "azurestorage_gen2: Blob already exists: #{blob_path}"
                else
                    raise_error "Creating blob '#{blob_path}' request failed - code: #{response.code}, body: #{response.body}, headers: #{response.headers}"
                end
            end
            request.run
        end

        private
        def append_blob_block(blob_path, content, position)
            log.debug "azurestorage_gen2: append_blob_block.start: Append blob ('#{blob_path}') called with position #{position} (content length: #{content.length}, end position: #{position + content.length})"
            datestamp = create_request_date
            headers = {:"x-ms-version" =>  ABFS_API_VERSION,  :"x-ms-date" => datestamp, :"Content-Length" => content.length}
            params = {:action => "append", :position => "#{position}"}
            auth_header = create_auth_header("patch", datestamp, "#{@azure_container}#{blob_path}", headers, params)
            headers[:Authorization] = auth_header
            req_opts = {
                :method => :patch,
                :params => params,
                :headers => headers,
                :body => content,
                :timeout => @http_timeout_seconds
            }
            add_proxy_options(req_opts)
            request = Typhoeus::Request.new("https://#{azure_storage_account}#{@url_domain_suffix}/#{@azure_container}#{blob_path}", req_opts)
            request.on_complete do |response|
                if response.success?
                    log.debug "azurestorage_gen2: Blob '#{blob_path}' has been appended, response code: #{response.code}"
                elsif response.timed_out?
                    raise_error  "Appending blob #{blob_path}' request timed out."
                elsif response.code == 404
                    raise AppendBlobResponseError.new("Blob '#{blob_path}' has not found. Error code: #{response.code}", 404)
                elsif response.code == 409
                    raise AppendBlobResponseError.new("Blob '#{blob_path}' has conflict. Error code: #{response.code}", 409)
                else
                    raise_error "Appending blob '#{blob_path}' request failed - code: #{response.code}, body: #{response.body}, headers: #{response.headers}"
                end
            end
            request.run
        end

        private
        def flush(blob_path, position)
            log.debug "azurestorage_gen2: flush_blob.start: Flush blob ('#{blob_path}') called with position #{position}"
            datestamp = create_request_date
            headers = {:"x-ms-version" => ABFS_API_VERSION, :"x-ms-date" => datestamp, :"Content-Length" => "0"}
            params = {:action => "flush", :position => "#{position}"}
            auth_header = create_auth_header("patch", datestamp, "#{@azure_container}#{blob_path}",headers, params)
            headers[:Authorization] = auth_header
            req_opts = {
                :method => :patch,
                :params => params,
                :headers => headers,
                :timeout => @http_timeout_seconds
            }
            add_proxy_options(req_opts)
            request = Typhoeus::Request.new("https://#{azure_storage_account}#{@url_domain_suffix}/#{@azure_container}#{blob_path}", req_opts)
            request.on_complete do |response|
                if response.success?
                    log.debug "azurestorage_gen2: Blob '#{blob_path}' flush was successful, response code: #{response.code}"
                elsif response.timed_out?
                    raise_error  "Bloub '#{blob_path}' flush request timed out."
                else
                    raise_error "Blob flush request failed - code: #{response.code}, body: #{response.body}, headers: #{response.headers}"
                end
            end
            request.run
        end

        private
        def get_blob_properties(blob_path)
            datestamp = create_request_date
            headers = {:"x-ms-version" =>  ABFS_API_VERSION, :"x-ms-date" => datestamp, :"Content-Length" => "0"}
            params = {}
            content_length = -1
            auth_header = create_auth_header("head", datestamp, "#{@azure_container}#{blob_path}", headers, params)
            headers[:Authorization] = auth_header
            req_opts = {
                :method => :head,
                :params => params,
                :headers => headers,
                :timeout => @http_timeout_seconds
            }
            add_proxy_options(req_opts)
            request = Typhoeus::Request.new("https://#{azure_storage_account}#{@url_domain_suffix}/#{@azure_container}#{blob_path}", req_opts)
            request.on_complete do |response|
                if response.success?
                  log.debug "azurestorage_gen2: Get blob properties for '#{blob_path}', response headers: #{response.headers}"
                  content_length = response.headers['Content-Length'].to_i
                elsif response.timed_out?
                    raise_error  "Get blob properties '#{blob_path}' request timed out."
                elsif response.code == 404
                    log.debug "azurestorage_gen2: Blob '#{blob_path}' does not exist. Creating it if needed..."
                    content_length = 0
                else
                    raise_error "Get blob properties '#{blob_path}' request failed - code: #{response.code}, body: #{response.body}, headers: #{response.headers}"
                end
            end
            request.run
            content_length
        end

        private
        def append_blob(content, chunk, existing_content_length)
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

        private
        def create_auth_header(method, datestamp, resource, headers, params)
            if @azure_storage_access_key.nil?
                "Bearer #{@azure_access_token}"
            else
                "SharedKey #{@azure_storage_account}:#{signed(method, datestamp, resource, headers, params)}"
            end
        end
        
        private
        def add_proxy_options(req_opts = {})
            unless @proxy_url.nil?
                req_opts[:proxy] = @proxy_url
                unless @proxy_username.nil? || @proxy_password.nil?
                    req_opts[:proxyuserpwd] = "#{@proxy_username}:#{@proxy_password}"
                end
            end
        end

        private
        def signed(method, datestamp, resource, headers, params)
            decoded_access_key=Base64.strict_decode64(@azure_storage_access_key).unpack("H*").first
            sign_request(decoded_access_key, signable_string(method, resource, params, headers, datestamp))
        end

        private
        def sign_request(key, signable_string)
            signed = OpenSSL::HMAC.digest('sha256', key, signable_string)
            Base64.strict_encode64(signed)
        end

        private
        def signable_string(method, resource, params, headers, datestamp)
            [
              method.to_s.upcase,
              headers.fetch("Content-Encoding", ""),
              headers.fetch("Content-Language", ""),
              headers.fetch("Content-Length", "").sub(/^0+/, ""),
              headers.fetch("Content-MD5", ""),
              headers.fetch("Content-Type", ""),
              headers.fetch("Date", ""),
              headers.fetch("If-Modified-Since", ""),
              headers.fetch("If-Match", ""),
              headers.fetch("If-None-Match", ""),
              headers.fetch("If-Unmodified-Since", ""),
              headers.fetch("Range", ""),
              "x-ms-date:#{datestamp}\nx-ms-version:#{ABFS_API_VERSION}",
              get_canonicalized_resource(resource, params)
            ].join("\n")
        end

        private
        def get_canonicalized_resource(resource, params)
            if params.empty?
                canonicalized_resource="/#{@azure_storage_account}"
            else
                canonicalized_params = params
                .map{|paramKey, paramValue| "#{paramKey.to_s.downcase}:#{paramValue}"}
                .join("\n")
                canonicalized_resource="/#{@azure_storage_account}/#{resource}\n#{canonicalized_params}"
            end
        end

        private
        def hex_to_bin(hex)
            hex = '0' << hex unless (hex.length % 2) == 0
            hex.scan(/[A-Fa-f0-9]{2}/).inject('') { |encoded, byte| encoded << [byte].pack('H*') }
        end

        private
        def create_request_date
            Time.now.strftime('%a, %e %b %y %H:%M:%S %Z')
        end

        private
        def raise_error(error_message)
            if @enable_retry
                raise BlobOperationError, error_message
            else
                raise Fluent::UnrecoverableError,  error_message
            end
        end

        def uuid_random
            require 'uuidtools'
            ::UUIDTools::UUID.random_create.to_s
        end

        def hex_random(chunk)
            unique_hex = Fluent::UniqueId.hex(chunk.unique_id)
            unique_hex.reverse!
            unique_hex[0...@hex_random_length]
        end
        
        def timekey_to_timeformat(timekey)
            case timekey
            when nil          then ''
            when 0...60       then '%Y%m%d%H%M%S' # 60 exclusive
            when 60...3600    then '%Y%m%d%H%M'
            when 3600...86400 then '%Y%m%d%H'
            else                   '%Y%m%d'
            end
        end

        class Compressor
            include Fluent::Configurable
      
            def initialize(opts = {})
              super()
              @buffer_type = opts[:buffer_type]
              @log = opts[:log]
            end
      
            attr_reader :buffer_type, :log
      
            def configure(conf)
              super
            end
      
            def ext
            end
      
            def content_type
            end
      
            def compress(chunk, tmp)
            end
      
            private
            def check_command(command, algo = nil)
              require 'open3'
      
              algo = command if algo.nil?
              begin
                Open3.capture3("#{command} -V")
              rescue Errno::ENOENT
                raise Fluent::ConfigError, "'#{command}' utility must be in PATH for #{algo} compression"
              end
            end
          end
      
        class GzipCompressor < Compressor
            def ext
              'gz'.freeze
            end
      
            def content_type
              'application/x-gzip'.freeze
            end
      
            def compress(chunk, tmp)
              w = Zlib::GzipWriter.new(tmp)
              chunk.write_to(w)
              w.finish
            ensure
              w.finish rescue nil
            end
        end
      
        class TextCompressor < Compressor
            def ext
              'txt'.freeze
            end
      
            def content_type
              'text/plain'.freeze
            end
      
            def compress(chunk, tmp)
              chunk.write_to(tmp)
            end
        end
      
        class JsonCompressor < TextCompressor
            def ext
              'json'.freeze
            end
      
            def content_type
              'application/json'.freeze
            end
        end
      
        COMPRESSOR_REGISTRY = Fluent::Registry.new(:azurestorage_compressor_type, 'fluent/plugin/azurestorage_gen2_compressor_')
        {
              'gzip' => GzipCompressor,
              'json' => JsonCompressor,
              'text' => TextCompressor
        }.each { |name, compressor|
            COMPRESSOR_REGISTRY.register(name, compressor)
        }
      
        def self.register_compressor(name, compressor)
            COMPRESSOR_REGISTRY.register(name, compressor)
        end

    end

    class AppendBlobResponseError < StandardError
        attr_reader :status_code
        def initialize(message="Default message", status_code=0)
          @status_code = status_code
          super(message)
        end
    end

    class BlobOperationError < StandardError
        def initialize(message="Default message")
            super(message)
        end
    end
end
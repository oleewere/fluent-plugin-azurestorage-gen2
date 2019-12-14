require 'net/http'
require 'json'
require 'tempfile'
require 'time'
require 'fluent/plugin/output'

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
            tmp = Tempfile.new("azure-")
            begin
                chunk.write_to(tmp)
                tmp.close
                generate_log_name(metadata, @current_index)
                if @last_azure_storage_path != @azure_storage_path
                    @current_index = 0
                    generate_log_name(metadata, @current_index)
                end
                content = File.open(tmp.path, 'rb') { |file| file.read }
                #append_blob(content, metadata)
                log.info "Metadata: #{metadata}"
                log.info "Storage path: #{@azure_storage_path}"
                upload(@azure_container, @azure_access_token, log)
                @last_azure_storage_path = @azure_storage_path
            ensure
                tmp.unlink
            end
        end

        def format(tag, time, record)
            r = inject_values_to_record(tag, time, record)
            @formatter.format(tag, time, r)
        end

        private
        def ensure_container
            container_exists(@azure_container, @azure_access_token, log)
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
            @azure_storage_path = extract_placeholders(storage_path, metadata)
        end

        def setup_access_token
            @get_token_lock = Concurrent::ReadWriteLock.new
            acquire_access_token
            if @azure_oauth_refresh_interval > 0
                log.info("Start getting access token every #{@azure_oauth_refresh_interval} seconds.")
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
            uri = URI('http://169.254.169.254/metadata/identity/oauth2/token')
            params = { :"api-version" => "2018-02-01", :resource => "https://storage.azure.com/" }
            unless @azure_instance_msi.nil?
                params[:msi_res_id] = @azure_instance_msi
            end
            uri.query = URI.encode_www_form(params)
  
            req = Net::HTTP::Get.new(uri)
            req['Metadata'] = "true"
  
            res = Net::HTTP.start(uri.hostname, uri.port) {|http|
                http.request(req)
            }
            if res.is_a?(Net::HTTPSuccess)
                data = JSON.parse(res.body)
                log.debug "Token response: #{data}"
                token = data["access_token"]
            else
                raise Fluent::UnrecoverableError, "Failed to acquire access token. #{res.code}: #{res.body}"
            end
            @azure_access_token = token
        end

        private
        def container_exists
            log.info "Check container exists"
        end
    
        private
        def create_container
            log.info "Create container"
        end
    
        private
        def upload
            log.info "Upload file #{@azure_access_token}"
        end

    end
end
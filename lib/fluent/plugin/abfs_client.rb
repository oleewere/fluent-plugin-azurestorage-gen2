module AbfsClient

    URL_DOMAIN_SUFFIX = '.dfs.core.windows.net'

    def self.container_exists(container, token)
        log.info "Check container exists"
    end

    def self.create_container(container, token)
        log.info "Create container"
    end

    def self.upload(token)
        log.info "Upload file #{token}"
    end

end
module AbfsClient

    URL_DOMAIN_SUFFIX = '.dfs.core.windows.net'

    def container_exists(container, token)
        log.info "Check container exists"
    end

    def create_container(container, token)
        log.info "Create container"
    end

    def upload(token)
        log.info "Upload file #{token}"
    end

end
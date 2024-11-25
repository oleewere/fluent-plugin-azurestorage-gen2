# Use the official Fluentd image from Docker Hub as the base image
FROM fluent/fluentd:v1.14-1

# Use root account to use apk
USER root

# Install plugins (Example: elasticsearch and kafka plugins)
RUN apk add --no-cache --update --virtual .build-deps \
        sudo build-base ruby-dev

# Copy custom configuration file (uncomment this if you have a custom configuration)
# COPY fluent.conf /fluentd/etc/

# Copy plugins configuration file (uncomment this if you have plugins configuration)
# COPY plugins /fluentd/plugins/

# Set the configuration file path environment variable
RUN apk add --no-cache git
RUN apk add --no-cache build-base libffi-dev linux-headers
RUN apk add --no-cache curl tar
ENV FLUENTD_CONF="fluent.conf"

RUN mkdir -p /var/log/fluent
RUN chown fluent:fluent /var/log/fluent

ENV COLUMNIFY_VERSION=0.1.1
RUN curl -L -o columnify.tar.gz https://github.com/reproio/columnify/releases/download/v${COLUMNIFY_VERSION}/columnify_${COLUMNIFY_VERSION}_Linux_x86_64.tar.gz \
    && tar -xzf columnify.tar.gz \
    && rm columnify.tar.gz \
    && mv columnify /usr/local/bin/columnify
ADD . /work
#RUN cd /work && sudo gem build /work/fluent-plugin-azurestorage-gen2.gemspec
#RUN cd /work && sudo gem install fluent-plugin-azurestorage-gen2*.gem
RUN sudo gem install /work/fluent-plugin-azurestorage-gen2-*.gem
# Change the user to fluentd
USER fluent

# Expose the port on which Fluentd will listen

# Start Fluentd
CMD ["fluentd", "-c", "/fluentd/etc/fluent.conf"]


# Azure Datalake Storage Gen2 Fluentd Output Plugin (IN PROGRESS)

[![Build Status](https://travis-ci.org/oleewere/fluent-plugin-azurestorage-gen2.svg?branch=master)](https://travis-ci.org/oleewere/fluent-plugin-azurestorage-gen2)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Gem Version](https://badge.fury.io/rb/fluent-plugin-azurestorage-gen2.svg)](http://badge.fury.io/rb/fluent-plugin-azurestorage-gen2)
![](https://ruby-gem-downloads-badge.herokuapp.com/fluent-plugin-azurestorage-gen2?extension=png)

## Requirements

| fluent-plugin-azurestorage-gen2 | fluentd | ruby |
|------------------------|---------|------|
| >= 0.1.0 | >= v0.14.0 | >= 2.4 |

## Overview

Fluent output plugin that can use ABFS api and append blobs with MSI support

## Installation

Install from RubyGems:
```
$ gem install fluent-plugin-azurestorage-gen2
```

## Configuration

```
<match **>
  @type azurestorage_gen2
  azure_storage_account    cloudbreakabfs
  azure_container          oszabo
  azure_instance_msi       /subscriptions/mysubscriptionid
  azure_object_key_format  %{path}-%{index}.%{file_extension}
  file_extension           log
  path                     "/cluster-logs/myfolder/${tag[1]}-#{Socket.gethostname}-%M"
  auto_create_container    true
</match>
```

### Configuration options

TODO

## TODOs

- add storage key support
- add compression (if append is not used)

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

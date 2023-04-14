
<h1 style="text-align:center;">RabbitMQ client for the stream protocol</h1>

---
<div style="text-align:center;">

[![Build Status](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/actions/workflows/main.yaml/badge.svg)](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/actions)
[![codecov](https://codecov.io/gh/rabbitmq/rabbitmq-stream-dotnet-client/branch/main/graph/badge.svg?token=OIA04ZQD79)](https://codecov.io/gh/rabbitmq/rabbitmq-stream-dotnet-client)
</div>




The RabbitMQ Stream .NET Client is a .NET library to communicate with the [RabbitMQ Stream Plugin](https://rabbitmq.com/stream.html). It allows to create and delete streams, as well as to publish to and consume from these streams.


The client is distributed via [NuGet](https://www.nuget.org/packages/RabbitMQ.Stream.Client/).

Please refer to the [documentation](https://rabbitmq.github.io/rabbitmq-stream-dotnet-client/stable/htmlsingle/index.html) ([PDF](https://rabbitmq.github.io/rabbitmq-stream-dotnet-client/stable/dotnet-stream-client.pdf)) to find out more.


### Support

* For questions: [RabbitMQ Users](https://groups.google.com/forum/#!forum/rabbitmq-users)
* For bugs and feature requests:  [GitHub Issues](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/issues)

## How to Use

### Pre-requisites

The library requires .NET 6 or .NET 7.

### Documentation


- [HTML documentation](https://rabbitmq.github.io/rabbitmq-stream-dotnet-client/stable/htmlsingle/index.html)
- [PDF documentation](https://rabbitmq.github.io/rabbitmq-stream-dotnet-client/stable/dotnet-stream-client.pdf)

- [A Simple Getting started](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/blob/main/docs/Documentation/)
- [Super Stream example](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/blob/main/docs/SuperStream)
- [Stream Performance Test](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/tree/main/RabbitMQ.Stream.Client.PerfTest)




## Build from source

Build:

```shell
make build
```

Test:

```shell
make test
```

To execute the tests you need a RabbitMQ `v3.11.9` running with the following plugins enabled:
- `rabbitmq_management`
- `rabbitmq_stream`
- `rabbitmq_stream_management`
- `rabbitmq_amqp1_0`


## Publish the documentation to github pages:

Make sure you are in the `main` branch

```shell
make publish-github-pages
```


## Release Process

* Ensure builds are green: [link](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/actions)
* Tag the `main` branch using your GPG key:
    ```
    git tag -a -s -u GPG_KEY_ID -m 'rabbitmq-stream-dotnet-client v1.0.0' 'v1.0.0' && git push && git push --tags
    ```
* Ensure the build for the tag passes: [link](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/actions)
* Create the new release on GitHub, which triggers a build and publish to NuGet: [link](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/releases)
* Check for the new version on NuGet: [link](https://www.nuget.org/packages/RabbitMQ.Stream.Client)
* Best practice is to download the new package and inspect the contents using [NuGetPackageExplorer](https://github.com/NuGetPackageExplorer/NuGetPackageExplorer)
* Announce the new release on the mailing list: [link](https://groups.google.com/g/rabbitmq-users)

[![Build Status](https://travis-ci.org/intelsdi-x/snap-plugin-publisher-kafka.svg?branch=master)](https://travis-ci.org/intelsdi-x/snap-plugin-publisher-kafka)

# Snap publisher plugin - Kafka

Allows publishing of data to [Apache Kafka](http://kafka.apache.org)

It's used in the [snap framework](http://github.com/intelsdi-x/snap).

1. [Getting Started](#getting-started)
  * [System Requirements](#system-requirements)
  * [Installation](#installation)
  * [Configuration and Usage](#configuration-and-usage)
2. [Documentation](#documentation)
  * [Kafka Quickstart](#kafka-quickstart)
  * [Published data](#published-data)
  * [Examples](#examples)
  * [Roadmap](#roadmap)
3. [Community Support](#community-support)
4. [Contributing](#contributing)
5. [License](#license)
6. [Acknowledgements](#acknowledgements)

## Getting Started

### System Requirements

- Uses [sarama](http://shopify.github.io/sarama/) golang client for Kafka by Shopify

### Installation

#### Download Kafka plugin binary:
You can get the pre-built binaries for your OS and architecture at plugin's [GitHub Releases](https://github.com/intelsdi-x/snap-plugin-publisher-kafka/releases) page.

#### To build the plugin binary:
Fork https://github.com/intelsdi-x/snap-plugin-publisher-kafka

Clone repo into `$GOPATH/src/github.com/intelsdi-x/`:

```
$ git clone https://github.com/<yourGithubID>/snap-plugin-publisher-kafka.git
```

Build the plugin by running make within the cloned repo:
```
$ make
```
This builds the plugin in `./build`

### Configuration and Usage
* Set up the [snap framework](https://github.com/intelsdi-x/snap/blob/master/README.md#getting-started)

#### Task Manifest Config

In task manifest, in config section of Kafka publisher the following settings can be declared:

| Key     | Type       | Default value     | Description                |
|---------|------------|-------------------|----------------------------|
| topic   | string     | "snap"            | The topic to send messages |
| brokers | string     | "localhost:9092"  | Semicolon delimited list of "server:port" brokers </br> Kafka's standard broker communication port is 9092 |


**Notice** To publish to remote Kafka endpoint, please ensure that all listed brokers are available from the host where snap-plugin-publisher-kafka is running. You can simply check it by running `telnet` providing address and port where Kafka advertised broker is running, for example `telnet 123.45.67.89 9092`.
## Documentation

### Kafka Quickstart

This is a minimal-configuration needed to run the Kafka broker service on Docker

#### Run ZooKeeper server on docker  
Kafka uses ZooKeeper so you need to first start a ZooKeeper server if you don't already have one.
```	
 $ docker run -d --name zookeeper jplock/zookeeper:3.4.6
```
Check if ZooKeeper docker is running:
```	
$ docker ps  

	CONTAINER ID        IMAGE                      COMMAND                CREATED             STATUS              PORTS              			NAMES
	9b0ddbdd75cd        jplock/zookeeper:3.4.6     "/opt/zookeeper/bin/   38 seconds ago      Up 38 seconds       2181/tcp, 2888/tcp, 3888/tcp	zookeeper
```  

#### Run Kafka server on docker
```
 docker run -d --name kafka --link zookeeper:zookeeper ches/kafka
```

#### Verify the running docker containers:
```	
$ docker ps

	CONTAINER ID        IMAGE                        COMMAND                CREATED             STATUS              PORTS							NAMES
	dfb3cdfb3f87        ches/kafka:latest            "/start.sh"            7 seconds ago       Up 6 seconds        7203/tcp, 9092/tcp				kafka
	9b0ddbdd75cd        jplock/zookeeper:3.4.6       "/opt/zookeeper/bin/   3 minutes ago       Up 3 minutes        2181/tcp, 2888/tcp, 3888/tcp	zookeeper
```

#### Get Kafka advertised hostname:
```	
$ docker inspect --format '{{ .NetworkSettings.IPAddress }}' kafka

  172.17.0.14
```

Read more about Kafka on [http://kafka.apache.org](http://kafka.apache.org/documentation.html)

### Published data

The plugin publishes all collected metrics serialized as JSON to Kafka. An example of published data is below:

```json
[
  {
    "timestamp": "2016-07-25T11:27:59.795548989+02:00",
    "namespace": "/intel/mock/bar",
    "data": 82,
    "unit": "",
    "tags": {
      "plugin_running_on": "my-machine"
    },
    "version": 0,
    "last_advertised_time": "2016-07-25T11:27:21.852064032+02:00"
  },
  {
    "timestamp": "2016-07-25T11:27:59.795549268+02:00",
    "namespace": "/intel/mock/foo",
    "data": 72,
    "unit": "",
    "tags": {
      "plugin_running_on": "my-machine"
    },
    "version": 0,
    "last_advertised_time": "2016-07-25T11:27:21.852063228+02:00"
  }
]
```

### Examples
Example of running [psutil collector plugin](https://github.com/intelsdi-x/snap-plugin-collector-psutil) and publishing data to Kafka.

Set up the [Snap framework](https://github.com/intelsdi-x/snap/blob/master/README.md#getting-started)

Ensure [Snap daemon is running](https://github.com/intelsdi-x/snap#running-snap):
* initd: `service snap-telemetry start`
* systemd: `systemctl start snap-telemetry`
* command line: `sudo snapteld -l 1 -t 0 &`

Download and load Snap plugins (paths to binary files for Linux/amd64):
```
$ wget http://snap.ci.snap-telemetry.io/plugins/snap-plugin-publisher-kafka/latest/linux/x86_64/snap-plugin-publisher-kafka
$ wget http://snap.ci.snap-telemetry.io/plugins/snap-plugin-collector-psutil/latest/linux/x86_64/snap-plugin-collector-psutil
$ snaptel plugin load snap-plugin-publisher-kafka
$ snaptel plugin load snap-plugin-collector-psutil
```

Create a [task manifest](https://github.com/intelsdi-x/snap/blob/master/docs/TASKS.md) (see [exemplary tasks](examples/tasks/)),
for example `psutil-kafka.json` with following content:
```json
{
  "version": 1,
  "schedule": {
    "type": "simple",
    "interval": "1s"
  },
  "workflow": {
    "collect": {
      "metrics": {
        "/intel/psutil/load/load1": {},
        "/intel/psutil/load/load15": {}
      },
      "publish": [
        {
          "plugin_name": "kafka",
          "config": {
            "topic": "test",
            "brokers": "172.17.0.14:9092"
          }
        }
      ]
    }
  }
}
```

Create a task:
```
$ snaptel task create -t psutil-kafka.json
```

Watch created task:
```
$ snaptel task watch <task_id>
```

To stop previously created task:
```
$ snaptel task stop <task_id>
```

### Roadmap

There isn't a current roadmap for this plugin, but it is in active development. As we launch this plugin, we do not have any outstanding requirements for the next release.

If you have a feature request, please add it as an [issue](https://github.com/intelsdi-x/snap-plugin-publisher-kafka/issues/new) and/or submit a [pull request](https://github.com/intelsdi-x/snap-plugin-publisher-kafka/pulls).

## Community Support
This repository is one of **many** plugins in **Snap**, a powerful telemetry framework. See the full project at http://github.com/intelsdi-x/snap To reach out to other users, head to the [main framework](https://github.com/intelsdi-x/snap#community-support)

## Contributing
We love contributions! 

There's more than one way to give back, from examples to blogs to code updates. See our recommended process in [CONTRIBUTING.md](CONTRIBUTING.md).

## License
[Snap](http://github.com/intelsdi-x/snap), along with this plugin, is an Open Source software released under the Apache 2.0 [License](LICENSE).

## Acknowledgements

* Author: [Nicholas Weaver](https://github.com/lynxbat)

And **thank you!** Your contribution, through code and participation, is incredibly important to us.

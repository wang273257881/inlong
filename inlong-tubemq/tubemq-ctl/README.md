
# TubeMQ-CTL

## Description

A command line tool facilitating the management and use of TubeMQ's Topic by users, supporting for message production and consumption.

## Installation

Apache Maven 3.6.3 and Java 8 required.

```
$ mvn clean install
```
Location of the executable file:

```
/target/client/bin

client
└── bin
    ├── tubemq-ctl.bat
    └── tubemq-ctl.sh
```
## Usage

Since using Unix OS, commands use `tubemq-ctl.sh`.

Please replace `tubemq-ctl.sh` with `tubemq-ctl.bat` if using Windows OS.

```
$ ./tubemq-ctl.sh {Type} {Action} [-Options]
```

Types contains topic and msg, their respective actions and options are as follows:

*action means action require that option

### Type-topic

#### Actions

- `cerate`: Create a topic with options.
- `delete`: Delete a specific topic.
- `update`: Modify topic information using options.
- `get`: Get a list of topics that meet options.
- `list`: Get a list of all topics.

#### Options

| Option | Description | Default Value | Valid Actions|
| --- | --- | --- | --- |
| `-acceptPublish` | Does the topic receive publishing requests | true | create, update |
| `-acceptSubscribe` | Does the topic receive subscription requests | true | create, update |
| `-brokerId` | BrokerId |  | *create, *delete, *update, get |
| `-confModAuthToken` | confModAuthToken |  | *create, *delete, *update |
| `-consumeGroup` | ConsumeGroup |  |  |
| `-createUser` | User creating topic |  | get |
| `-deletePolicy` | DeletePolicy | Config of broker | create, update, get |
| `-deleteWhen` | Time to delete topic | Config of broker | create, update, get |
| `-help` | Help |  |  |
| `-master` | Ip and port of master node |  |  |
| `-maxMsgSizeInMB` | Maximum message packet length setting | 1 | update |
| `-memCacheFlushIntvl` | Maximum allowed waiting refresh interval for memory cache | 20000 | create, update, get |
| `-memCacheMsgCntInK` | Default maximum memory cache packet size | 10 | create, update, get |
| `-memCacheMsgSizeInMB` | The total size of the default memory cache package | 2 | create, update, get |
| `-modifyUser` | User modifying topic |  | get |
| `-msg` | Message to produce |  |  |
| `-numPartitions` | Partition size | Config of broker | create, update, get |
| `-numTopicStores` | Number of Topic data blocks and partition management groups allowed to be established | 1 | create, update, get |
| `-topic` | Topic to produce or consume |  |  |
| `-topicName` | TopicName |  | *create, *delete, *update, get |
| `-topicStatusId` | Status of the topic,0: normal, 1: soft deleted, 2: hard deleted | 0 | get |
| `-unflushDataHold` | Default maximum allowed size of data to be refreshed | 0 | create, update, get |
| `-unflushInterval` | Maximum allowed time interval to be refreshed | Config of broker | create, update, get |
| `-unflushThreshold` | Maximum allowed number of records to be refreshed | Config of broker | create, update, get |

### Type-msg

#### Actions

- `produce`: Produce a message.
- `consume`: Consume messages.

#### Options

| Option | Description | Default Value | Valid Actions|
| --- | --- | --- | --- |
| `-consumeGroup` | ConsumeGroup |  | *consume |
| `-help` | Help |  |  |
| `-master` | Ip and port of master node |  | *produce, *consume |
| `-msg` | Message to produce |  | *produce |
| `-topic` | Topic to produce or consume |  | *produce, *consume |

### Others

#### Options

| Option | Description | Default Value | Valid Actions|
| --- | --- | --- | --- |
| `-help` | Help |  |  |

## Examples

### Create topic

```
$ ./tubemq-ctl.sh topic create -brokerId 1 -topicName ts1 -confModAuthToken abc
```

### Get all topic

```
$ ./tubemq-ctl.sh topic list
```

### Produce a message

```
$ ./tubemq-ctl.sh msg produce -master 127.0.0.1:8715 -topic ts1 -msg test
```

### Consume messages

```
$ ./tubemq-ctl.sh msg consume -master 127.0.0.1:8715 -topic ts1 -consumeGroup con1
```

### Get help

```
$ ./tubemq-ctl.sh -help
```

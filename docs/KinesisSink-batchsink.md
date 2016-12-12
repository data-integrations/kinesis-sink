# Amazon Kinesis Stream Batch Sink


Description
-----------
Kinesis sink that outputs to a specified Amazon Kinesis Stream.

Use Case
--------
This sink is used when you want to write to a kinesis stream.

co.cask.hydrator.plugin.batch.Properties
----------
**name:** The name of the kinesis stream to output to. Must be a valid kinesis stream name. The kinesis stream will be
created if it does not exist.

**accessID:** The access Id provided by AWS required to access the kinesis streams.

**accessKey:** AWS access key secret having access to Kinesis streams.

**distribute:** Boolean to decide if the data has to be uniformly distributed among all the shards or has to be sent to
a single shard.

**shardCount:** Number of shards to be created, each shard has input of 1MB/s. Default value is 1. If the stream already
exists, number of shards will not be modified.

Example
-------
This example will write to a kinesis stream named 'MyKinesisStream'. The kinesis stream will be created if it does not
exists already. Each record it receives will be written as a single stream event. Two shards for this stream will be
created and records will be distributed uniformly across the records::

    {
        "name": "MyKinesisStream",
        "type": "KinesisSink",
        "properties": {
            "name": "purchases",
            "accessID": "my_aws_access_key",
            "accessKey": "my_aws_access_secret",
            "shardCount": "2",
            "distribute": "true"
        }
    }

import boto3
import json

client = boto3.client('kinesis')
stream = client.describe_stream(StreamName='datastream')
print(stream)
shardId = stream['StreamDescription']['Shards'][1]["ShardId"]
print(shardId)

iterator = client.get_shard_iterator(StreamName='datastream', ShardId=shardId, ShardIteratorType='TRIM_HORIZON')
print(iterator)

records = client.get_records(ShardIterator=iterator['ShardIterator'])

print(len(records['Records']))


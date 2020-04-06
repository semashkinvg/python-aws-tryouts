import boto3
import pandas as pd
import json
import time
import sys

streamName = 'datastream'
client = boto3.client('kinesis')
stream = client.describe_stream(StreamName=streamName)
print(stream)

columns = {'Financial Security': 'object'}
df = pd.read_csv("../data/oil-gas-other-regulated-wells-beginning-1860.csv", dtype=columns).fillna(
    0)

df['json'] = df.apply(lambda x: x.to_json())
print(df.columns[25])
print(df.iloc[2])

cnt = df.shape[0]
i = 0

batchSize = 500
records = [{'Data': json.dumps(r), 'PartitionKey': str(r['County Code'])} for r in
           df.to_dict(orient='records')]
for i in range(0, cnt, batchSize):
    start_time = time.time()
    batch = records[i:i + batchSize]
    client.put_records(StreamName='datastream',
                       Records=batch)
    print('range {i}:{j} took {time} seconds {size}'.format(size=sys.getsizeof(batch) ,time=(time.time() - start_time), i=i, j=i + batchSize))

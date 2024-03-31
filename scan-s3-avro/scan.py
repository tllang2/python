import boto3
from fastavro import reader
import json
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor

ids_that_i_care = [
    "XY-1-ABCDEFGHIJKL"
]

id_map = {}
for id in ids_that_i_care:
    id_map[id] = True

result = open("s3-result-test.json", 'w')

# Specify your S3 path
s3_path = 's3://tllang-test/abc.xyz/year=2024/month=03/day=20/hour=05/'

s3 = boto3.client('s3')

# List objects recursively in the specified S3 path
response = s3.list_objects_v2(Bucket=s3_path.split('/')[2], Prefix=s3_path.split('/', 3)[-1])

def process_avro_file(file_name):
    # Download the Avro file from S3
    response = s3.get_object(Bucket=s3_path.split('/')[2], Key=file_name)
    avro_data = response['Body'].read()

    # Read Avro records from the downloaded data
    avro_stream = BytesIO(avro_data)
    avro_reader = reader(avro_stream)

    print("reading", file_name)

    for record in avro_reader:
        record['After'] = json.loads(record['After'])
        record['Before'] = json.loads(record['Before'])
        
        if record['Before'] is not None and record['Before']['id'] not in id_map:
            continue
        if record['After'] is not None and record['After']['id'] not in id_map:
            continue

        del record['Schema']
        record['Source'] = json.loads(record['Source'])
        record['Metadata'] = str(record['Metadata'])
        
        result.write(file_name)
        result.write('\n')
        result.write(json.dumps(record))
        result.write('\n')

# Use ThreadPoolExecutor to parallelize file processing
with ThreadPoolExecutor(max_workers=5) as executor:
    executor.map(process_avro_file, [obj['Key'] for obj in response.get('Contents', [])])

result.close()

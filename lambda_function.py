import csv
import json

import boto3


def lambda_handler(event, context):
    event_body = json.loads(event['body'])
    print(event_body)
    league = event_body["league"]
    user = event_body.get("user")
    s3_bucket = "football-table-predictor-dev"
    s3_key = f"{league}/{f'{user}_table' if user else 'table'}.csv"
    print(f'Looking for: {s3_key}')

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_bucket)
    output_file = f"/tmp/{s3_key.split('/')[-1]}"
    bucket.download_file(s3_key, output_file)
    print(f"Downloaded file to {output_file}")

    table = []
    with open(output_file, 'r') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            table.append(row)
    
    return json.dumps(table)


if __name__ == '__main__':
    print(lambda_handler({
        "league": "en1",
        "user": "haydn"
    }, None))

import boto3

# credentials, default region is taken from env variable
# if not ~/.aws/credentials
s3 = boto3.resource('s3')

# Print out bucket names
for bucket in s3.buckets.all():
    print(bucket.name)


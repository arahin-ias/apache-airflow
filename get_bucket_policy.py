from airflow.providers.amazon.aws.hooks.s3 import S3Hook

s3_hook = S3Hook(aws_conn_id='aws_conn_id')
bucket = s3_hook.get_bucket('alpha-test-bucket-rahin')
policy_resource = bucket.Policy()

policy_json = policy_resource.policy

print(policy_json)
import boto3


def shutdown_instance():
    ec2 = boto3.client('ec2', region_name='us-east-2')
    instance_id = 'i-045ac35dbc8d2e530'
    #log_activity("Initiating shutdown of instance.")
    ec2.stop_instances(InstanceIds=[instance_id])
    #log_activity("Instance stopped.")


if __name__ == '__main__':
    shutdown_instance()
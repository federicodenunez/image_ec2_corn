import boto3
import time

def shutdown_instance():
    ec2 = boto3.client('ec2', region_name='us-east-2')  # Ajusta la región
    instance_id = 'i-045ac35dbc8d2e530'
    print("Shutting down instance...")
    ec2.stop_instances(InstanceIds=[instance_id])
    print("Instance stopped.")

def pipeline_main():
  #daily_data_main()
  #q_agent_main()
  #bot_main()
  print("Prueba")

if __name__ == '__main__':
    pipeline_main()  
    shutdown_instance()

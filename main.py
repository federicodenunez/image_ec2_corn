import logging
import watchtower
import boto3
from datetime import datetime, timezone, time as dtime

# Set up the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# Create a CloudWatch log handler
handler = watchtower.CloudWatchLogHandler(
    log_group="EC2ActivityLog",
    region_name='us-east-2'
)
logger.addHandler(handler)

def log_activity(message):
    now = datetime.now(timezone.utc)
    log_line = f"{now.isoformat()} - {message}"
    logger.info(log_line)

def shutdown_instance():
    ec2 = boto3.client('ec2', region_name='us-east-2')
    instance_id = 'i-045ac35dbc8d2e530'
    log_activity("Initiating shutdown of instance.")
    ec2.stop_instances(InstanceIds=[instance_id])
    log_activity("Instance stopped.")

def pipeline_main():
    print("Prueba")
    # Here, replace with your actual pipeline actions and record the action.
    log_activity("Pipeline executed: did nothing xd.")

if __name__ == '__main__':
    inicio_intervalo = dtime(7, 55)
    fin_intervalo = dtime(8, 20)
    ahora_utc = datetime.now(timezone.utc).time()

    if inicio_intervalo <= ahora_utc <= fin_intervalo:
        pipeline_main()  
        shutdown_instance()
    else:
        log_activity("Admin started instance manually at non-scheduled time.")
        print("Bienvenido administrador, no se ejecutó el pipeline con el booteo.")

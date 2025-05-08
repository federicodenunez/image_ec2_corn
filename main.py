import os
import logging
import watchtower
import boto3
import asyncio
from datetime import datetime, timezone, time as dtime
from descarga import download_and_process_forecast
from ibkr_bot import bot


# Set the default region via an environment variable
os.environ["AWS_DEFAULT_REGION"] = "us-east-2"

# # Set up the logger
# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)
# # Create a CloudWatch log handler without passing region_name
# handler = watchtower.CloudWatchLogHandler(log_group="EC2ActivityLog")
# logger.addHandler(handler)

# def log_activity(message):
#     now = datetime.now(timezone.utc)
#     log_line = f"{now.isoformat()} - {message}"
#     logger.info(log_line)

def shutdown_instance():
    ec2 = boto3.client('ec2', region_name='us-east-2')
    instance_id = 'i-045ac35dbc8d2e530'
    #log_activity("Initiating shutdown of instance.")
    ec2.stop_instances(InstanceIds=[instance_id])
    #log_activity("Instance stopped.")


if __name__ == '__main__':
    inicio_intervalo = dtime(17, 45)
    fin_intervalo = dtime(18, 20)
    ahora_utc = datetime.now(timezone.utc).time()

    if inicio_intervalo <= ahora_utc <= fin_intervalo:
        download_and_process_forecast()  
        #flag = agente("forecasts.npz", "corn_price_data.csv")
        #asyncio.run(bot(flag))
        #log_activity("Everything executed correctly.")
        shutdown_instance()
    else:
        #log_activity("Admin started instance manually at non-scheduled time.")
        download_and_process_forecast()  
        print("Bienvenido administrador, no se ejecutó el pipeline con el booteo.")

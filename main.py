import boto3
import time
from datetime import datetime, time as dtime

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
    # Definir el intervalo de tiempo (UTC)
    inicio_intervalo = dtime(7, 55)
    fin_intervalo = dtime(8, 20)

    # Obtener la hora actual en UTC
    ahora_utc = datetime.utcnow().time()

    # Verificar si la hora actual está entre 7:55 y 8:20
    if inicio_intervalo <= ahora_utc <= fin_intervalo:
        pipeline_main()  
        shutdown_instance()
    else:
        print("Bienvenido administrador, no se ejecutó el pipeline con el booteo.")

        
import os
import boto3
from datetime import datetime, timezone, time as dtime
from descarga import download_and_process_forecast
from ibkr_bot import bot, conseguir_precio_hoy, market_open, ib_disconnect


# Set the default region via an environment variable
os.environ["AWS_DEFAULT_REGION"] = "us-east-2"

def publish_heartbeat(): # Aviso a la alarma que se corrió bien. Si no llega, me manda alerta
    cw = boto3.client('cloudwatch')
    cw.put_metric_data(
        Namespace='Custom/MainPy',
        MetricData=[{
            'MetricName': 'RunSuccess',
            'Value': 1,
            'Unit': 'Count'
        }]
    )

if __name__ == '__main__':
    inicio_intervalo = dtime(17, 45)
    fin_intervalo = dtime(18, 20)
    ahora_utc = datetime.now(timezone.utc).time()

    if inicio_intervalo <= ahora_utc <= fin_intervalo:
        download_and_process_forecast()  
        ib = conseguir_precio_hoy()
        #modelo("corn_price_data.csv", "forecasts.npz")
        if market_open():
            bot(ib)
        ib_disconnect(ib)
        publish_heartbeat()

    else:
        print("Bienvenido administrador, no se ejecutó el pipeline con el booteo.")

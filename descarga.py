import os
from datetime import datetime, timezone, timedelta
import numpy as np
import xarray as xr
from ecmwf.opendata import Client
import pandas as pd
import joblib
import time
import yfinance as yf

def filter_half_deg_grid(df):
    # Keep only lat/lon with .0 or .5 (i.e., not .25 or .75)
    def is_half_or_whole(val):
        frac = round(val % 1, 2)
        return frac == 0.0 or frac == 0.5

    return df[df["latitude"].apply(is_half_or_whole) & df["longitude"].apply(is_half_or_whole)]

def convert_to_original_format(df, var_name="t2m", height=2.0):
    """
    Convert a dataframe with columns:
    ['time', 'step', 'latitude', 'longitude', 'valid_time', var_name]
    into:
    ['step', 'latitude', 'longitude', 'time', 'heightAboveGround', 'valid_time', var_name]
    """
    df = df.copy()

    # Convert longitudes from -180 to 180 to 0–360
    df["longitude"] = df["longitude"].apply(lambda x: x + 360 if x < 0 else x)

    # Reorder columns to match original format
    column_order = ["time", "step", "latitude", "longitude", "valid_time", var_name]
    df = df[column_order]

    return df

def procesar_archivo(grib_file):
    desired_steps = [np.timedelta64(i, 'h') for i in range(0, 361, 6)]
    #"area": "50/255/35/280", es nuestra area original de la descarga de training.
    # Cambió el formato de 0:360 a -180:180 -> tenemos que hacer la reconversión para descargar los datos.
    lat_min, lat_max = 35.0, 50.0
    lon_min, lon_max = -105.0, -80.0

    os.makedirs("temperatura", exist_ok=True)
    os.makedirs("precipitaciones", exist_ok=True)

    found_any = False

    # --- t2m (2m temperature) ---
    try:
        ds_t2m = xr.open_dataset(
            grib_file,
            engine="cfgrib",
            backend_kwargs={
                "filter_by_keys": {
                    "shortName": "2t",
                    #"typeOfLevel": "heightAboveGround",
                    #"level": 2
                }
            },
        )
        ds_t2m = ds_t2m.sel(
            step=desired_steps,
            latitude=slice(lat_max, lat_min),
            longitude=slice(lon_min, lon_max)
        )
        df_t2m = ds_t2m["t2m"].to_dataframe().reset_index()
        # Filter to only .0 or .5 degree grid
        df_t2m = filter_half_deg_grid(df_t2m)
        df_t2m = convert_to_original_format(df_t2m, "t2m")

        df_t2m.to_csv(f"temperatura/{os.path.basename(grib_file.split('.')[0])}.csv", index=False)
        print("✅ Saved t2m to temperatura/")
        found_any = True
    except Exception as e:
        print(f"⚠️ Failed to process t2m: {e}")

    # --- tp (total precipitation) ---
    try:
        ds_tp = xr.open_dataset(
            grib_file,
            engine="cfgrib",
            backend_kwargs={
                "filter_by_keys": {
                    "shortName": "tp",
                    #"typeOfLevel": "surface"
                }
            },
        )
        ds_tp = ds_tp.sel(
            step=desired_steps,
            latitude=slice(lat_max, lat_min),
            longitude=slice(lon_min, lon_max)
        )
        df_tp = ds_tp["tp"].to_dataframe().reset_index()
        # Filter to only .0 or .5 degree grid
        df_tp = filter_half_deg_grid(df_tp)
        df_tp = convert_to_original_format(df_tp, "tp")

        df_tp.to_csv(f"precipitaciones/{os.path.basename(grib_file.split('.')[0])}.csv", index=False)
        print("✅ Saved tp to precipitaciones/")
        found_any = True
    except Exception as e:
        print(f"⚠️ Failed to process tp: {e}")

    if not found_any:
        raise ValueError("❌ Could not process t2m or tp from the GRIB file.")
    
def download(today):
    """
    Descarga el grib
    """
    client = Client(
        source="ecmwf",
        model='aifs-single',
    )

    os.makedirs("gribs", exist_ok=True)
    grib_file = f"gribs/{today}.grib2"

    if os.path.exists(grib_file):
        print("🟡 Forecast already downloaded, skipping download.")
    else:
        print(f"📥 Downloading 12 UTC ECMWF HRES forecast for {today}...")
        client.retrieve( # AIFS 17:45
            date= (today).strftime('%Y-%m-%d'), # indico la fecha de lo que quiero
            ### AYER: - timedelta(days=1)).strftime('%Y-%m-%d')
            time= 12, # indico la hora de publicacion del forecast que quiero
            #stream= "enfo", y type="cf" para control forecast
            type= "fc", # QUIERO LLEGAR A AIFS_SINGLE
            param= ["2t", "tp"], #2t y tp tienen el mismo código de parametro que el que descargamos nosotros aunque cambie el nombre
            step= [0, 6, 12, 18, 24, 30, 36, 42, 48, 54, 60, 66, 72, 78, 84, 90, 96, 102, 108, 114, 120, 126, 132, 138, 144, 150, 156, 162, 168, 174, 180, 186, 192, 198, 204, 210, 216, 222, 228, 234, 240, 246, 252, 258, 264, 270, 276, 282, 288, 294, 300, 306, 312, 318, 324, 330, 336, 342, 348, 354, 360],
            target= grib_file,
        )

        print("✅ Download complete.")

def tensorear(today):
    # Tensorea todo de chill
    df_temp = pd.read_csv(f"temperatura/{today}.csv") 
    df_prcp = pd.read_csv(f"precipitaciones/{today}.csv") 
    
    df_temp['latitude'] = ((df_temp['latitude'] - 35.0) / 0.5).astype(int) ##llevo las latitudes al rango [0,30]
    df_temp['longitude'] = ((df_temp['longitude'] - 255.0) / 0.5).astype(int) ##llevo las longitudes al rango [0,50]
    df_temp['step'] = pd.to_timedelta(df_temp['step'])
    df_temp['step'] = (df_temp['step'].dt.total_seconds() / (6 * 3600)).astype(int) ##llevo los steps al rango [0,60]
    
    df_prcp['latitude'] = ((df_prcp['latitude'] - 35.0) / 0.5).astype(int) ##llevo las latitudes al rango [0,30]
    df_prcp['longitude'] = ((df_prcp['longitude'] - 255.0) / 0.5).astype(int) ##llevo las longitudes al rango [0,50]
    df_prcp['step'] = pd.to_timedelta(df_prcp['step'])
    df_prcp['step'] = (df_prcp['step'].dt.total_seconds() / (6 * 3600)).astype(int) ##llevo los steps al rango [0,60]
    
    times = df_temp['time'].unique() # deberian ser 61
    for time in times: # el for va a ser una sola vez
        #llevo el df temperaturas a un tensor con la forma 61,31,51 respetando el orden step x latitude x longitude
        df_temp_time = df_temp[df_temp['time'] == time] ##selecciono un dia en especifico
        df_temp_time = df_temp_time.sort_values(by=['step', 'latitude', 'longitude'])
        tensor_df = df_temp_time.pivot_table(index=['step', 'latitude', 'longitude'], values='t2m')
        tensor_df = tensor_df.sort_index() 
        tensor_temp = tensor_df.to_numpy()
        tensor_temp = tensor_temp.reshape(61,31,51)
        # le saco el step 0
        tensor_temp = tensor_temp[1:] # saco el step 0:0

        #llevo el df precipitaciones a un tensor con la forma 61,31,51 respetando el orden step x latitude x longitude
        df_prcp_time = df_prcp[df_prcp['time'] == time] ##selecciono un dia en especifico
        df_prcp_time = df_prcp_time.sort_values(by=['step', 'latitude', 'longitude'])
        tensor_df = df_prcp_time.pivot_table(index=['step', 'latitude', 'longitude'], values='tp')
        tensor_df = tensor_df.sort_index() 
        tensor_prcp = tensor_df.to_numpy()
        tensor_prcp = tensor_prcp.reshape(61,31,51)

        # trabajo el tensor de hoy
        tensor_prcp = np.diff(tensor_prcp, axis=0, prepend=tensor_prcp[[0]])
        tensor_prcp = np.clip(tensor_prcp, 0, None)
        tensor_prcp = tensor_prcp[1:] # saco el step 0:0

        forecast = np.stack((tensor_temp, tensor_prcp), axis=-1) # 60x51x32x2
        forecast = forecast.reshape(15, 4, 31, 51, 2)  # 15 días, 4 steps/día
        forecast = forecast.reshape(15, -1)  # flatten cada día

        keys = []
        vectors = []
        formatted_time = datetime.strptime(time, "%Y-%m-%d %H:%M:%S").strftime("%d-%m-%Y")
        for i in range(15):
                    day_key = f"{formatted_time}-{i+1}"
                    keys.append(day_key)
                    vectors.append(forecast[i])



        vectors = np.array(vectors)
        yeo_johnson = joblib.load("pkl/yeo-johnson.pkl")
        scaler = joblib.load("pkl/scaler.pkl")
        pca = joblib.load("pkl/pca.pkl")
        vectors_procesado = scaler.transform(pca.transform(yeo_johnson.transform(vectors))) # me quedan 15 vectores de 435 componentes principales escalados

        # Inicializo forecasts vacío o lo cargo si ya existe
        if os.path.exists("forecasts.npz"):
            forecasts_npz = np.load("forecasts.npz", allow_pickle=True)
            forecasts = {}
            for key in forecasts_npz.keys():
                forecasts[key] = forecasts_npz[key]
        else:
            forecasts = {}

        # Inicializo forecasts vacío o lo cargo si ya existe RAW
        if os.path.exists("forecasts_raw.npz"):
            forecasts_npz_raw = np.load("forecasts_raw.npz", allow_pickle=True)
            forecasts_raw = {}
            for key in forecasts_npz_raw.keys():
                forecasts_raw[key] = forecasts_npz_raw[key]
        else:
            forecasts_raw = {}
        

        # sumo los nuevos
        for k,v in zip(keys, vectors_procesado):
            forecasts[k] = v

        # sumo los nuevos RAW
        for k,v in zip(keys, vectors):
            forecasts_raw[k] = v
            
        
        # reescribo el archivo con los nuevos
        np.savez("forecasts.npz", **forecasts)
        np.savez("forecasts_raw.npz", **forecasts_raw)

def eliminar_archivos():
    # Elimina todos los archivos de hoy, dejando solo el npz
    # Elimina todo lo de las carpetas que se usan cada día, dejandolas vacías para el próximo.
    
    # Borrar GRIBs y .idx
    for filename in os.listdir("gribs"):
        path = os.path.join("gribs", filename)
        if filename.endswith(".grib2") or filename.endswith(".idx"):
            try:
                os.remove(path)
                print(f"🗑️ Eliminado: {path}")
            except Exception as e:
                print(f"⚠️ No se pudo eliminar {path}: {e}")

    # Borrar CSVs de temperatura
    for filename in os.listdir("temperatura"):
        path = os.path.join("temperatura", filename)
        if filename.endswith(".csv"):
            try:
                os.remove(path)
                print(f"🗑️ Eliminado CSV temperatura: {path}")
            except Exception as e:
                print(f"⚠️ No se pudo eliminar {path}: {e}")

    # Borrar CSVs de precipitaciones
    for filename in os.listdir("precipitaciones"):
        path = os.path.join("precipitaciones", filename)
        if filename.endswith(".csv"):
            try:
                os.remove(path)
                print(f"🗑️ Eliminado CSV precipitaciones: {path}")
            except Exception as e:
                print(f"⚠️ No se pudo eliminar {path}: {e}")

def update_price_in_csv(csv, today):
    # Load CSV and ensure correct dtypes
    df = pd.read_csv(csv)
    df['date'] = pd.to_datetime(df['date'])

    today_str = today.strftime('%Y-%m-%d')
    today_row = df[df['date'] == pd.to_datetime(today_str)]

    if today_row.empty:
        print(f"Date {today_str} not found in CSV.")
        return

    # Check if today is a weekend or holiday
    if today_row.iloc[0]['weekend_or_holiday'] == 1:
        # Find the most recent non-weekend/holiday row *before* today
        previous_rows = df[df['date'] < pd.to_datetime(today_str)]
        last_valid_row = previous_rows[previous_rows['weekend_or_holiday'] == 0].tail(1)
        
        if last_valid_row.empty or pd.isna(last_valid_row.iloc[0]['price']):
            print("No previous valid trading day price available.")
            return

        last_price = last_valid_row.iloc[0]['price']
        df.loc[df['date'] == pd.to_datetime(today_str), 'price'] = last_price
        df.loc[df['date'] == pd.to_datetime(today_str), 'return'] = 0
        df.loc[df['date'] == pd.to_datetime(today_str), 'logreturn'] = 0
        df.to_csv(csv, index=False)
        print(f"{today_str} is a weekend or holiday. Filled price with previous value: {last_price}")
        return

    # Else: regular trading day — download price from Yahoo Finance
    ticker = yf.Ticker("ZC=F")
    data = ticker.history(period="1d", interval="1m")

    if data.empty:
        print("No data received from Yahoo Finance.")
        return

    latest_price = data['Close'].iloc[-1]
    df.loc[df['date'] == pd.to_datetime(today_str), 'price'] = latest_price

    # Try to find yesterday's valid trading day
    previous_rows = df[df['date'] < pd.to_datetime(today_str)]
    last_valid_row = previous_rows[previous_rows['weekend_or_holiday'] == 0].tail(1)

    if not last_valid_row.empty and not pd.isna(last_valid_row.iloc[0]['price']):
        price_yesterday = last_valid_row.iloc[0]['price']
        ret = (latest_price - price_yesterday) / price_yesterday
        logret = np.log1p(ret)
        df.loc[df['date'] == pd.to_datetime(today_str), 'return'] = ret
        df.loc[df['date'] == pd.to_datetime(today_str), 'logreturn'] = logret
        print(f"Price: {latest_price}, Return: {ret}, LogReturn: {logret}")
    else:
        print("Previous valid price not found — skipping return/logreturn.")

    df.to_csv(csv, index=False)
    print(f"Price of {today_str}: {latest_price} was successfully uploaded.")


def download_and_process_forecast():

    start_time = time.time()

    today = datetime.now(timezone.utc).date() - timedelta(days=1)
    grib_file = f"gribs/{today}.grib2"

    print("Descargando GRIB de hoy...")
    download(today)
    procesar_archivo(grib_file)
    print("Tensoreando pronósticos de hoy")
    tensorear(today) 
    print("Tensores añadidos al npz")
    print("Eliminando archivos")
    #eliminar_archivos()

    #update_price_in_csv("corn_price_data.csv", today)

    print("✅ All done!")
    print(f"Descarga y tensoreo ejecutados en: {time.time() - start_time:.2f} segundos")

def ver_npz():
    forecasts_npz = np.load("forecasts.npz")

    print("Contenido de forecasts.npz:")
    for key in forecasts_npz.files:
        array = forecasts_npz[key]
        print(f" → {key}: shape={array.shape}, dtype={array.dtype}")

if __name__ == "__main__":
    download_and_process_forecast()
    # ver_npz()
    

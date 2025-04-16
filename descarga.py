import os
from datetime import datetime, timezone, timedelta
import numpy as np
import xarray as xr
from ecmwf.opendata import Client

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

    # Reorder and rename columns as needed
    #df["heightAboveGround"] = height

    # Convert longitudes from 0–360 to -180 to 180
    #df["longitude"] = df["longitude"].apply(lambda x: x - 360 if x > 180 else x)

    # Reorder columns to match original format
    column_order = ["time", "step", "latitude", "longitude", "valid_time", var_name]
    df = df[column_order]

    return df

def procesar_archivo(grib_file):
    desired_steps = [np.timedelta64(i, 'h') for i in range(0, 361, 6)]
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
    
def download():
    """
    Descarga el grib
    """
    today = datetime.now(timezone.utc).date()
    client = Client(
        source="ecmwf",
        #model='aifs-single',
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
        # client.retrieve({ # IFS 20:40
        #     "date"   : (today-timedelta(days=1)).strftime('%Y-%m-%d'),  # ESTA PUESTO AYER
        #     "time"   : 12,
        #     "type"   : "cf",            # miembro de control del ENS físico
        #     "step"   : list(range(0,361,6)),  # [0,6,...,360]
        #     "param"  : ["2t","tp"],
        #     #"area"   : [50, -105, 35, -80],   # Norte, Oeste, Sur, Este
        #     #"format" : "grib2",
        # }, target=grib_file)
        # client.retrieve( # IFS 20"40
        #     date= (today - timedelta(days=1)).strftime('%Y-%m-%d'), # indico la fecha de lo que quiero
        #     ### AYER: - timedelta(days=1)).strftime('%Y-%m-%d')
        #     time= 12, # indico la hora de publicacion del forecast que quiero
        #     #stream= "enfo", #y type="cf" para control forecast
        #     type= "cf", # QUIERO LLEGAR A AIFS_SINGLE
        #     param= ["2t", "tp"], #2t y tp tienen el mismo código de parametro que el que descargamos nosotros aunque cambie el nombre
        #     step= [0, 6, 12, 18, 24, 30, 36, 42, 48, 54, 60, 66, 72, 78, 84, 90, 96, 102, 108, 114, 120, 126, 132, 138, 144, 150, 156, 162, 168, 174, 180, 186, 192, 198, 204, 210, 216, 222, 228, 234, 240, 246, 252, 258, 264, 270, 276, 282, 288, 294, 300, 306, 312, 318, 324, 330, 336, 342, 348, 354, 360],
        #     target= grib_file,
        # )

        print("✅ Download complete.")

def tensorear():
    # Tensorea todo de chill
    return 

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

    

def download_and_process_forecast():
    today = datetime.now(timezone.utc).date()
    grib_file = f"gribs/{today}.grib2"

    print("Descargando GRIB de hoy...")
    download()
    procesar_archivo(grib_file)
    print("Tensoreando pronósticos de hoy")
    #tensorear() # tensorear, normalizar y añadir los pronósticos de hoy al tensor completo.
    print("Tensores añadidos al npz")
    print("Eliminando archivos")
    #eliminar_archivos()
    print("✅ All done!")

if __name__ == "__main__":
    download_and_process_forecast()
    

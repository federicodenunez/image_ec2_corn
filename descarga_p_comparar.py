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

    # Detectar modelo a partir del nombre de archivo
    model_subdir = "aifs" if "aifs-single" in grib_file else "ifs"

    # Crear subcarpetas
    os.makedirs(f"temperatura/{model_subdir}", exist_ok=True)
    os.makedirs(f"precipitaciones/{model_subdir}", exist_ok=True)

    base_filename = os.path.basename(grib_file.split('.')[0])

    found_any = False

    # --- t2m (2m temperature) ---
    try:
        ds_t2m = xr.open_dataset(
            grib_file,
            engine="cfgrib",
            backend_kwargs={"filter_by_keys": {"shortName": "2t"}},
        )
        ds_t2m = ds_t2m.sel(
            step=desired_steps,
            latitude=slice(lat_max, lat_min),
            longitude=slice(lon_min, lon_max)
        )
        df_t2m = ds_t2m["t2m"].to_dataframe().reset_index()
        df_t2m = filter_half_deg_grid(df_t2m)
        df_t2m = convert_to_original_format(df_t2m, "t2m")

        df_t2m.to_csv(f"temperatura/{model_subdir}/{base_filename}.csv", index=False)
        print(f"✅ Saved t2m to temperatura/{model_subdir}/")
        found_any = True
    except Exception as e:
        print(f"⚠️ Failed to process t2m: {e}")

    # --- tp (total precipitation) ---
    try:
        ds_tp = xr.open_dataset(
            grib_file,
            engine="cfgrib",
            backend_kwargs={"filter_by_keys": {"shortName": "tp"}},
        )
        ds_tp = ds_tp.sel(
            step=desired_steps,
            latitude=slice(lat_max, lat_min),
            longitude=slice(lon_min, lon_max)
        )
        df_tp = ds_tp["tp"].to_dataframe().reset_index()
        df_tp = filter_half_deg_grid(df_tp)
        df_tp = convert_to_original_format(df_tp, "tp")

        df_tp.to_csv(f"precipitaciones/{model_subdir}/{base_filename}.csv", index=False)
        print(f"✅ Saved tp to precipitaciones/{model_subdir}/")
        found_any = True
    except Exception as e:
        print(f"⚠️ Failed to process tp: {e}")

    if not found_any:
        raise ValueError("❌ Could not process t2m or tp from the GRIB file.")

def download():
    today = datetime.now(timezone.utc).date()
    os.makedirs("gribs", exist_ok=True)

    for offset in range(1, 2):  # ayer, anteayer, etc.
        forecast_date = today - timedelta(offset)

        for model_name, model_type in [("aifs-single", "fc"), ("ifs", "cf")]:
            client = Client(source="ecmwf", model=model_name)
            grib_file = f"gribs/{forecast_date}_{model_name}.grib2"

            if os.path.exists(grib_file):
                print(f"🟡 {model_name.upper()} forecast for {forecast_date} already exists. Skipping.")
                continue

            print(f"📥 Downloading {model_name.upper()} forecast for {forecast_date}...")
            client.retrieve(
                date=forecast_date.strftime('%Y-%m-%d'),
                time=12,
                type=model_type,
                param=["2t", "tp"],
                step=list(range(0, 361, 6)),
                target=grib_file,
            )
            print(f"✅ Downloaded {grib_file}")
            procesar_archivo(grib_file)

def main():
    today = datetime.now(timezone.utc).date()
    grib_file = f"gribs/{today}.grib2"

    print("Descargando GRIB de hoy...")
    download()
    procesar_archivo(grib_file)
    print("Tensoreando pronósticos de hoy")
    #tensorear() # tensorear, normalizar y añadir los pronósticos de hoy al tensor completo.
    #print("Tensores añadidos al npz")
    #print("Eliminando archivos")
    #eliminar_archivos()
    print("✅ All done!")

if __name__ == "__main__":
    main()
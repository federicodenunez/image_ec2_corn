import xarray as xr
import numpy as np
import os

def procesar_archivo(grib_file):
    # Forecast steps to keep: every 6h up to 15 days
    desired_steps = [np.timedelta64(i, 'h') for i in range(0, 361, 6)]

    # Corn Belt region
    lat_min, lat_max = 35.0, 50.0
    lon_min, lon_max = -105.0, -80.0

    # Output folders
    os.makedirs("temperatura", exist_ok=True)
    os.makedirs("precipitaciones", exist_ok=True)

    found_any = False

    # --- t2m (2m temperature) from Dataset 2 ---
    try:
        ds_t2m = xr.open_dataset(
            grib_file,
            engine="cfgrib",
            backend_kwargs={
                "filter_by_keys": {
                    "shortName": "t2m",
                    "typeOfLevel": "heightAboveGround",
                    "level": 2
                }
            },
        )
        ds_t2m = ds_t2m.sel(
            #step=desired_steps,
            latitude=slice(lat_max, lat_min),
            longitude=slice(lon_min, lon_max)
        )
        df_t2m = ds_t2m["t2m"].to_dataframe().reset_index()
        df_t2m.to_csv(f"temperatura/{os.path.basename(grib_file)}.csv", index=False)
        print("✅ Saved t2m to temperatura/")
        found_any = True
    except Exception as e:
        print(f"⚠️ Failed to process t2m: {e}")

    # --- tp (total precipitation) from Dataset 9 ---
    try:
        ds_tp = xr.open_dataset(
            grib_file,
            engine="cfgrib",
            backend_kwargs={
                "filter_by_keys": {
                    "shortName": "tp",
                    "typeOfLevel": "surface"
                }
            },
        )
        ds_tp = ds_tp.sel(
            #step=desired_steps,
            latitude=slice(lat_max, lat_min),
            longitude=slice(lon_min, lon_max)
        )
        df_tp = ds_tp["tp"].to_dataframe().reset_index()
        df_tp.to_csv(f"precipitaciones/{os.path.basename(grib_file)}.csv", index=False)
        print("✅ Saved tp to precipitaciones/")
        found_any = True
    except Exception as e:
        print(f"⚠️ Failed to process tp: {e}")

    if not found_any:
        raise ValueError("❌ Could not process t2m or tp from the GRIB file.")


procesar_archivo("/Users/federicodenunez/Documents/GitHub/image_ec2_corn/gribs/ecmwf_hres_2025-04-10.grib2")
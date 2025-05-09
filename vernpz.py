import numpy as np

def ver_npz():
    forecasts_npz = np.load("forecasts_raw.npz")

    print("Contenido de forecasts.npz:")
    for key in forecasts_npz.files:
        array = forecasts_npz[key]
        print(f" → {key}: shape={array.shape}, dtype={array.dtype}")
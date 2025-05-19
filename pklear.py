import numpy as np
import joblib

# Cargar modelos
yeo_johnson = joblib.load("pkl/yeo-johnson.pkl")
scaler = joblib.load("pkl/scaler.pkl")
pca = joblib.load("pkl/pca.pkl")

# Cargar forecasts_raw
forecasts_raw_npz = np.load("forecasts_raw.npz", allow_pickle=True)
forecasts_raw = {key: forecasts_raw_npz[key] for key in forecasts_raw_npz}

# Procesar
forecasts = {}
for k, v in forecasts_raw.items():
    v = v.reshape(1, -1)
    v_proc = scaler.transform(pca.transform(yeo_johnson.transform(v)))
    forecasts[k] = v_proc# .squeeze() # no se pq puso squeeze

# Guardar forecasts procesados
np.savez("forecasts.npz", **forecasts)
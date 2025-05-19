import numpy as np
import joblib

# Cargo los modelos
yeo_johnson = joblib.load("pkl/yeo-johnson.pkl")
scaler = joblib.load("pkl/scaler.pkl")
pca = joblib.load("pkl/pca.pkl")

# Cargo forecasts raw existentes
forecasts_raw_npz = np.load("forecasts_raw.npz", allow_pickle=True)

# Proceso cada vector con los modelos cargados
forecasts_procesados = {
    k: scaler.transform(pca.transform(yeo_johnson.transform(v.reshape(1, -1)))).squeeze()
    for k, v in forecasts_raw_npz.items()
}

# Guardo forecasts procesados sobrescribiendo el archivo anterior
np.savez("forecasts.npz", **forecasts_procesados)

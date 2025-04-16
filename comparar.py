import os
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timezone, timedelta

# Directorios base
DIR_TEMP = "temperatura"
DIR_PREC = "precipitaciones"

# Días disponibles para comparar
# Agregá manualmente o extendé este listado con los días disponibles
DAYS_TO_COMPARE = ["2025-04-14", "2025-04-15"]

# Archivos que vamos a comparar
comparaciones = []

# Construimos la lista de pares a comparar
def build_comparaciones():
    for dia in DAYS_TO_COMPARE:
        aifs_temp = os.path.join(DIR_TEMP, "aifs", f"{dia}_aifs-single.csv")
        ifs_temp = os.path.join(DIR_TEMP, "ifs", f"{dia}_ifs.csv")
        aifs_prcp = os.path.join(DIR_PREC, "aifs", f"{dia}_aifs-single.csv")
        ifs_prcp = os.path.join(DIR_PREC, "ifs", f"{dia}_ifs.csv")

        if os.path.exists(aifs_temp) and os.path.exists(ifs_temp) and os.path.exists(aifs_prcp) and os.path.exists(ifs_prcp):
            comparaciones.append((dia, aifs_temp, aifs_prcp, ifs_temp, ifs_prcp))

# Función de comparación

def comparar_datos_forecast(archivo_aifs_temp, archivo_aifs_prcp, archivo_ifs_temp, archivo_ifs_prcp, dia):
    print(f"\n📅 Comparación para el día {dia}")

    df_aifs_t2m = pd.read_csv(archivo_aifs_temp)
    df_aifs_tp = pd.read_csv(archivo_aifs_prcp)
    df_ifs_t2m = pd.read_csv(archivo_ifs_temp)
    df_ifs_tp = pd.read_csv(archivo_ifs_prcp)

    print("\n🌡️ TEMPERATURA A 2M (t2m)")
    print("AIFS:")
    print(df_aifs_t2m["t2m"].describe())
    print("\nIFS:")
    print(df_ifs_t2m["t2m"].describe())

    plt.figure(figsize=(10, 5))
    plt.hist(df_ifs_t2m["t2m"], bins=50, alpha=0.6, label="IFS t2m")
    plt.hist(df_aifs_t2m["t2m"], bins=50, alpha=0.6, label="AIFS t2m")
    plt.title(f"Distribuci\u00f3n Temperatura a 2m - {dia}")
    plt.xlabel("t2m (K)")
    plt.ylabel("Frecuencia")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    mean_diff_t2m = df_aifs_t2m["t2m"].mean() - df_ifs_t2m["t2m"].mean()
    print(f"📐 Diferencia en la media (AIFS - IFS) t2m: {mean_diff_t2m:.4f} K\n")

    print("🌧️ PRECIPITACI\u00d3N TOTAL (tp)")
    print("AIFS:")
    print(df_aifs_tp["tp"].describe())
    print("\nIFS:")
    print(df_ifs_tp["tp"].describe())

    plt.figure(figsize=(10, 5))
    plt.hist(df_ifs_tp["tp"], bins=50, alpha=0.6, label="IFS tp")
    plt.hist(df_aifs_tp["tp"], bins=50, alpha=0.6, label="AIFS tp")
    plt.title(f"Distribuci\u00f3n Precipitaci\u00f3n Total - {dia}")
    plt.xlabel("tp (mm)")
    plt.ylabel("Frecuencia")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    mean_diff_tp = df_aifs_tp["tp"].mean() - df_ifs_tp["tp"].mean()
    print(f"📐 Diferencia en la media (AIFS - IFS) tp: {mean_diff_tp:.4f} mm")

# Ejecutar
if __name__ == "__main__":
    build_comparaciones()
    for dia, aifs_temp, aifs_prcp, ifs_temp, ifs_prcp in comparaciones:
        comparar_datos_forecast(aifs_temp, aifs_prcp, ifs_temp, ifs_prcp, dia)

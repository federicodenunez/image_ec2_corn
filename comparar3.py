import os
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timezone, timedelta
from matplotlib.backends.backend_pdf import PdfPages

# Directorios base
DIR_TEMP = "temperatura"
DIR_PREC = "precipitaciones"

# Días disponibles para comparar
DAYS_TO_COMPARE = ["2025-04-14", "2025-04-15", "2025-04-16"]

# Almacenamiento de resultados
df_summary_rows = []

# Crear PDF para guardar gráficas
pdf_path = "comparacion_aifs_ifs.pdf"
pdf = PdfPages(pdf_path)

# Comparar y guardar resultados
def comparar_datos_forecast(archivo_aifs_temp, archivo_aifs_prcp, archivo_ifs_temp, archivo_ifs_prcp, dia):
    print(f"\n📅 Comparación para el día {dia}")

    df_aifs_t2m = pd.read_csv(archivo_aifs_temp)
    df_aifs_tp = pd.read_csv(archivo_aifs_prcp)
    df_aifs_tp["tp"] = df_aifs_tp["tp"] / 1000  # Convertir de kg/m² a para alinear con unidad de IFS
    df_ifs_t2m = pd.read_csv(archivo_ifs_temp)
    df_ifs_tp = pd.read_csv(archivo_ifs_prcp)

    # Estadísticas t2m
    mean_aifs_t2m = df_aifs_t2m["t2m"].mean()
    mean_ifs_t2m = df_ifs_t2m["t2m"].mean()
    diff_t2m = mean_aifs_t2m - mean_ifs_t2m
    max_aifs_t2m = df_aifs_t2m["t2m"].max()
    max_ifs_t2m = df_ifs_t2m["t2m"].max()
    min_aifs_t2m = df_aifs_t2m["t2m"].min()
    min_ifs_t2m = df_ifs_t2m["t2m"].min()

    # Estadísticas tp
    mean_aifs_tp = df_aifs_tp["tp"].mean()
    mean_ifs_tp = df_ifs_tp["tp"].mean()
    diff_tp = mean_aifs_tp - mean_ifs_tp
    max_aifs_tp = df_aifs_tp["tp"].max()
    max_ifs_tp = df_ifs_tp["tp"].max()
    min_aifs_tp = df_aifs_tp["tp"].min()
    min_ifs_tp = df_ifs_tp["tp"].min()

    df_summary_rows.append({
        "Fecha": dia,
        "Media AIFS t2m (K)": mean_aifs_t2m,
        "Media IFS t2m (K)": mean_ifs_t2m,
        "Diferencia t2m (K)": diff_t2m,
        "Max AIFS t2m (K)": max_aifs_t2m,
        "Max IFS t2m (K)": max_ifs_t2m,
        "Min AIFS t2m (K)": min_aifs_t2m,
        "Min IFS t2m (K)": min_ifs_t2m,
        "Media AIFS tp (mm)": mean_aifs_tp,
        "Media IFS tp (mm)": mean_ifs_tp,
        "Diferencia tp (mm)": diff_tp,
        "Max AIFS tp (mm)": max_aifs_tp,
        "Max IFS tp (mm)": max_ifs_tp,
        "Min AIFS tp (mm)": min_aifs_tp,
        "Min IFS tp (mm)": min_ifs_tp,
    })

    # Gráfico T2M
    plt.figure(figsize=(8, 4))
    plt.hist(df_ifs_t2m["t2m"], bins=50, alpha=0.6, label="IFS t2m")
    plt.hist(df_aifs_t2m["t2m"], bins=50, alpha=0.6, label="AIFS t2m")
    plt.title(f"Distribución T2M - {dia}")
    plt.xlabel("t2m (K)")
    plt.ylabel("Frecuencia")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    pdf.savefig()
    plt.close()

    # Gráfico TP
    plt.figure(figsize=(8, 4))
    plt.hist(df_ifs_tp["tp"], bins=50, alpha=0.6, label="IFS tp")
    plt.hist(df_aifs_tp["tp"], bins=50, alpha=0.6, label="AIFS tp")
    plt.title(f"Distribución TP - {dia}")
    plt.xlabel("tp (mm)")
    plt.ylabel("Frecuencia")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    pdf.savefig()
    plt.close()

# Ejecutar comparaciones
if __name__ == "__main__":
    for dia in DAYS_TO_COMPARE:
        aifs_temp = os.path.join(DIR_TEMP, "aifs", f"{dia}_aifs-single.csv")
        ifs_temp = os.path.join(DIR_TEMP, "ifs", f"{dia}_ifs.csv")
        aifs_prcp = os.path.join(DIR_PREC, "aifs", f"{dia}_aifs-single.csv")
        ifs_prcp = os.path.join(DIR_PREC, "ifs", f"{dia}_ifs.csv")

        if all(os.path.exists(f) for f in [aifs_temp, ifs_temp, aifs_prcp, ifs_prcp]):
            comparar_datos_forecast(aifs_temp, aifs_prcp, ifs_temp, ifs_prcp, dia)

    # Cerrar PDF
    pdf.close()

    # Crear DataFrame con resultados
    df_summary = pd.DataFrame(df_summary_rows)

    # Agregar promedio general
    promedio = df_summary.mean(numeric_only=True).to_dict()
    promedio["Fecha"] = "Promedio"
    df_summary = pd.concat([df_summary, pd.DataFrame([promedio])], ignore_index=True)

    # Guardar resumen en CSV
    df_summary.to_csv("comparacion_aifs_ifs_resumen_completo.csv", index=False)
    print("✅ Comparaciones guardadas en comparacion_aifs_ifs_resumen_completo.csv y comparacion_aifs_ifs.pdf")
import subprocess

def check_gpu():
    try:
        # Ejecuta el comando nvidia-smi para obtener información de la GPU
        output = subprocess.check_output("nvidia-smi", shell=True, stderr=subprocess.STDOUT)
        print("¡GPU detectada!")
        print(output.decode("utf-8"))
    except subprocess.CalledProcessError as e:
        # Si el comando falla, es probable que no haya GPU o que nvidia-smi no esté instalado
        print("No se detectó GPU. Detalles:")
        print(e.output.decode("utf-8"))
    except FileNotFoundError:
        # En caso de que el comando nvidia-smi no se encuentre
        print("El comando 'nvidia-smi' no se encontró. Es posible que no tengas instalados los drivers de NVIDIA o que no dispongas de GPU.")

if __name__ == "__main__":
    check_gpu()

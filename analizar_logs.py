import sys
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker


def analizar_y_graficar(nombre_archivo_log):

    print(f"--- Analizando el archivo: {nombre_archivo_log} ---")

    try:
        df = pd.read_csv(
            nombre_archivo_log,
            sep=' - ',
            header=None,
            names=['timestamp', 'tipo_evento', 'id_pregunta'],
            engine='python'
        )
    except FileNotFoundError:
        print(f"Error: El archivo '{nombre_archivo_log}' no fue encontrado.")
        return
    except Exception as e:
        print(f"Ocurrió un error al leer el archivo: {e}")
        return

    df['es_hit'] = (df['tipo_evento'] == 'HIT').astype(int)
    df['es_miss'] = (df['tipo_evento'] == 'MISS').astype(int)

    df['hits_acumulados'] = df['es_hit'].cumsum()
    df['misses_acumulados'] = df['es_miss'].cumsum()
    df['peticiones_totales'] = df.index + 1

    df['hit_rate'] = (df['hits_acumulados'] / df['peticiones_totales']) * 100

    total_hits = df['hits_acumulados'].iloc[-1]
    total_misses = df['misses_acumulados'].iloc[-1]
    hit_rate_final = df['hit_rate'].iloc[-1]

    print(f"Análisis completado.")
    print(f"Total de Peticiones: {len(df)}")
    print(f"Total de Cache Hits: {total_hits}")
    print(f"Total de Cache Misses: {total_misses}")
    print(f"Hit-Rate Final: {hit_rate_final:.2f}%")

    plt.style.use('ggplot')
    fig, ax = plt.subplots(figsize=(12, 7))

    ax.plot(df['peticiones_totales'].values, df['hit_rate'].values,
            label='Hit-Rate Evolutivo', color='dodgerblue')

    tipo_distribucion = nombre_archivo_log.replace('.log', '').capitalize()
    ax.set_title(
        f'Evolución del Hit-Rate de la Caché (Distribución {tipo_distribucion})', fontsize=16, fontweight='bold')
    ax.set_xlabel('Número de Peticiones', fontsize=12)
    ax.set_ylabel('Hit-Rate (%)', fontsize=12)

    ax.yaxis.set_major_formatter(mticker.PercentFormatter())

    ax.set_ylim(0, 100)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)
    ax.legend()

    nombre_archivo_salida = f'hit_rate_{tipo_distribucion.lower()}.png'
    plt.savefig(nombre_archivo_salida, dpi=300, bbox_inches='tight')

    print(f"¡Gráfico guardado exitosamente como '{nombre_archivo_salida}'!\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python3 analizar_logs.py <archivo_log_1> <archivo_log_2> ...")
        sys.exit(1)

    for archivo in sys.argv[1:]:
        analizar_y_graficar(archivo)

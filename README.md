# Plataforma Distribuida para Análisis de Respuestas con Caché

Este proyecto implementa una plataforma de software distribuida, basada en una arquitectura de microservicios, para analizar el rendimiento de un sistema de caché bajo diferentes patrones de carga. El sistema utiliza un dataset de Yahoo! Answers y simula la generación de respuestas para evaluar la efectividad de la caché en memoria (Redis) para reducir la carga sobre una base de datos persistente (PostgreSQL).

## Arquitectura del Sistema

El sistema está compuesto por varios servicios independientes, orquestados por Docker Compose:

- **`storage` (PostgreSQL):** Base de datos relacional que almacena las 10,000 preguntas y respuestas del dataset, así como las métricas generadas (`score`, `access_count`).
- **`db-loader`:** Un servicio de un solo uso que espera a que `storage` esté listo y luego lo puebla con los datos iniciales del archivo `test.csv`.
- **`cache-db` (Redis):** Base de datos en memoria de alta velocidad que almacena en caché las respuestas a las preguntas ya procesadas.
- **`cache-logic`:** El punto de entrada (API Gateway) del sistema. Recibe las peticiones, consulta la caché de Redis y, en caso de un _cache miss_, delega la tarea al `llm-module`.
- **`llm-module`:** Servicio que simula la generación de una respuesta por parte de un LLM. En esta implementación, utiliza la respuesta humana original del dataset para garantizar estabilidad. También calcula el `score` de similitud y actualiza las métricas en PostgreSQL.
- **`traffic-generator`:** Simula el comportamiento de los usuarios, enviando peticiones al sistema según patrones de tráfico configurables (uniforme o sesgado).

## Requisitos Previos

Asegúrate de tener instaladas las siguientes herramientas en tu sistema:

- **Docker:** [Guía de instalación de Docker](https://docs.docker.com/engine/install/)
- **Docker Compose:** [Guía de instalación de Docker Compose](https://docs.docker.com/compose/install/) (generalmente se incluye con Docker Desktop).

Para sistemas Linux, es posible que necesites ejecutar los comandos de Docker con `sudo`.

## Configuración Inicial

1.  **Clona el repositorio:**

    ```bash
    git clone <URL_DEL_REPOSITORIO>
    cd tarea1-sd
    ```

2.  **(Opcional) Clave de API:** Aunque el sistema final simula el LLM, la configuración para usar una API real de Google Gemini sigue presente. Si deseas experimentar con ella en el futuro, crea un archivo llamado `.env` en la raíz del proyecto:
    ```
    # .env
    GEMINI_API_KEY="TU_API_KEY_VA_AQUI"
    ```
    El sistema funcionará perfectamente sin este archivo, gracias al `llm-module` simulado.

## Uso Básico del Sistema

### Iniciar el Sistema

Para construir las imágenes de Docker y levantar todos los servicios en segundo plano, ejecuta:

```bash
sudo docker compose up --build -d

### Ver los logs en tiempo real
sudo docker compose up --build

### Detener el sistema
sudo docker compose down -v

## Ejecutar un Experimento de Larga Duración

### Configurar el Experimento (archivo docker-compose.yml) para tráfico uniforme o sesgado.

environment:
  - TRAFFIC_DISTRIBUTION=uniforme

environment:
  - TRAFFIC_DISTRIBUTION=sesgada

### Lanzar y Capturar Logs (se generan archivos .log en la carpeta madre).

sudo docker compose up --build -d

sudo docker compose logs -f cache-logic > nombre_archivo.log

sudo docker compose down -v

### Generar tráficos de rendimiento en gráficos.

pip install pandas matplotlib "numpy<2"

python3 analizar_logs.py uniforme.log sesgada.log

### Monitoreo y Consultas en Vivo

sudo docker compose ps

# Contar Cache Hits
sudo docker compose logs cache-logic | grep "HIT" | wc -l

# Contar Cache Misses
sudo docker compose logs cache-logic | grep "MISS" | wc -l

### Conectarse a la base de datos PostgreSQL

sudo docker compose exec storage psql -U myuser -d yahoo_answers

-- Contar el total de preguntas cargadas
SELECT COUNT(*) FROM questions;

-- Ver las 5 preguntas más procesadas (con mayor access_count)
SELECT id, access_count, score, question_title FROM questions ORDER BY access_count DESC LIMIT 5;

-- Ver las 5 preguntas con mayor score (en modo simulado, todas serán 1.0)
SELECT id, score, question_title FROM questions WHERE score IS NOT NULL ORDER BY score DESC LIMIT 5;


### Estructura fque debiese tener.


tarea1-sd/
├── cache/
│   ├── Dockerfile
│   ├── main.py
│   └── requirements.txt
├── data/
│   ├── classes.txt
│   ├── readme.txt
│   └── test.csv
├── db-loader/
│   ├── Dockerfile
│   ├── load_data.py
│   └── requirements.txt
├── llm-module/
│   ├── check_models.py
│   ├── Dockerfile
│   ├── main.py
│   └── requirements.txt
├── postgres-data/
│   └── (Datos persistentes de PostgreSQL)
├── traffic-generator/
│   ├── Dockerfile
│   ├── main.py
│   └── requirements.txt
├── .env (Opcional)
├── analizar_logs.py
├── docker-compose.yml
├── hit_rate_sesgada.png  (Generado)
├── hit_rate_uniforme.png (Generado)
├── sesgada.log           (Generado)
└── uniforme.log          (Generado)
```

## Arquitectura del Pipeline Asíncrono

El flujo de datos sigue el siguiente camino:

1.  **Ingesta:** `traffic-generator` llama (GET) al `cache-logic`.
2.  **Producción (Cache Miss):** `cache-logic` (Productor) envía el `question_id` al tópico de Kafka `preguntas`.
3.  **Consumo (LLM):** `llm_consumer` (Consumidor) toma el `question_id` de `preguntas`.
4.  **Procesamiento (LLM):** `llm_consumer` consulta PostgreSQL (para obtener el texto) y llama a la API de Google Gemini.
5.  **Bifurcación (LLM):**
    - **Éxito:** `llm_consumer` (Productor) envía la respuesta humana y la del LLM al tópico `respuestas`.
    - **Fallo (Rate Limit):** `llm_consumer` (Productor) envía el `question_id` al tópico `fallidas`.
6.  **Procesamiento (Flink):** Un _job_ de PyFlink (`ScoreCalculatorJob`) consume de `respuestas`, calcula el score con `spacy`.
7.  **Bifurcación (Flink):**
    - **Score Alto (>= 0.8):** El _job_ produce la respuesta + score al tópico `validadas`.
    - **Score Bajo (< 0.8):** El _job_ produce el `question_id` de vuelta al tópico `preguntas` (Feedback Loop).
8.  **Persistencia:** `storage_module` (Consumidor) toma los mensajes de `validadas` y los guarda en PostgreSQL.
9.  **Fallback:** `fallback_module` (Consumidor) toma los mensajes de `fallidas`, espera, y los re-encola en `preguntas`.
10. **Monitoreo:** `prometheus` recolecta métricas de `cache-logic` y `llm_consumer`, y `grafana` las visualiza.

jecución (Hard Reset)\*\*

Debido a la complejidad del clúster de Flink y la caché de construcción de Docker, es crucial forzar una re-creación limpia de los contenedores:

```bash
# Paso 1: Detener y eliminar cualquier contenedor/red existente
docker compose down

# Paso 2: Construir las imágenes y forzar la re-creación de los contenedores
docker compose up --build --force-recreate -d

#Vizualización de todos los logs

docker compose logs -f traffic_generator
docker compose logs -f cache_service
docker compose logs -f llm_consumer
docker compose logs -f flink-job-runner
docker compose logs -f flink-taskmanager
docker compose logs -f flink-jobmanager
docker compose logs -f storage_module
docker compose logs -f prometheus
docker compose logs -f grafana
docker compose logs -f kafka
docker compose logs -f postgres_db
docker compose logs -f db_loader

#Páginas accesibles

#Apache Flink

http://localhost:8081

#Prometheus

http://localhost:9090/targets

#Grafana

http://localhost:3000
```

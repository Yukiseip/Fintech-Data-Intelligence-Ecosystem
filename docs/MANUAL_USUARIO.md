# 📚 Manual de Usuario — Fintech Data Intelligence Platform

> **Versión:** 3.0 Final | **Última actualización:** Abril 2026

---

## 📖 Tabla de Contenidos

1. [¿Qué es este sistema?](#1-qué-es-este-sistema)
2. [Arquitectura en Resumen](#2-arquitectura-en-resumen)
3. [Requisitos Previos](#3-requisitos-previos)
4. [Instalación y Primer Arranque](#4-instalación-y-primer-arranque)
5. [Generación de Datos](#5-generación-de-datos)
6. [Ejecutar el Pipeline](#6-ejecutar-el-pipeline)
7. [Dashboard Streamlit](#7-dashboard-streamlit)
8. [Monitoreo con Grafana](#8-monitoreo-con-grafana)
9. [Airflow — Orquestación](#9-airflow--orquestación)
10. [dbt — Transformaciones SQL](#10-dbt--transformaciones-sql)
11. [Pruebas](#11-pruebas)
12. [Comandos Make de Referencia](#12-comandos-make-de-referencia)
13. [Solución de Problemas Comunes](#13-solución-de-problemas-comunes)
14. [Resumen Visual del Flujo](#14-resumen-visual-del-flujo)

---

## 1. ¿Qué es este sistema?

La **Fintech Data Intelligence Platform** es un pipeline de datos completo que:

- **Genera** transacciones financieras sintéticas con patrones de fraude inyectados
- **Procesa** los datos en 3 capas (Bronze → Silver → Gold) usando Apache Spark
- **Detecta fraude** automáticamente con 3 reglas de negocio + un modelo ML (Isolation Forest)
- **Visualiza** los datos en tiempo casi real en un dashboard Streamlit y dashboards Grafana
- **Orquesta** todo el proceso con Apache Airflow ejecutando un DAG cada hora

### ¿Qué aprenderás / podrás demostrar con este proyecto?

| Área | Tecnología |
|------|-----------|
| Ingeniería de datos | Spark, Medallion Architecture, dbt, Parquet |
| MLOps | scikit-learn Isolation Forest, scoring batch |
| DevOps | Docker Compose, Terraform, Airflow |
| Calidad de datos | Great Expectations, dbt tests |
| Observabilidad | Prometheus, Grafana |
| Seguridad | PII hashing, RBAC, env vars |

---

## 2. Arquitectura en Resumen

```
Datos brutos (JSONL)
        ↓
   [S3 LocalStack]
        ↓
   BRONZE LAYER   ← Spark job: bronze_ingest.py
   (Parquet raw)
        ↓
   SILVER LAYER   ← Spark job: silver_cleanse.py
   (Parquet limpio, sin PII)
        ↓
   GOLD LAYER     ← Spark job: gold_modeling.py → PostgreSQL
   (Star Schema)
        ↓
   FRAUDE         ← anomaly_detection.py + ml_scoring.py → alerts_log
        ↓
   dbt            ← mart_executive_kpis, mart_fraud_summary (vistas analíticas)
        ↓
   Streamlit      ← Dashboard en http://localhost:8501
   Grafana        ← Dashboards en http://localhost:3000
```

---

## 3. Requisitos Previos

### Software requerido

| Herramienta | Versión mínima | Verificación |
|-------------|---------------|--------------|
| Docker Desktop | 24.x+ | `docker --version` |
| Docker Compose | 2.x+ | `docker compose version` |
| Make | Cualquiera | `make --version` |
| Git | 2.x+ | `git --version` |

> ⚠️ **En Windows**: Usa **WSL 2** o **Git Bash** para ejecutar comandos `make`.
> PowerShell no es compatible con el Makefile directamente.

### Recursos de hardware recomendados

| Recurso | Mínimo | Recomendado |
|---------|--------|-------------|
| RAM | 8 GB libres | 16 GB |
| CPU | 4 núcleos | 8 núcleos |
| Disco | 10 GB libres | 20 GB |

---

## 4. Instalación y Primer Arranque

### Paso 1 — Clonar el repositorio

```bash
git clone <url-del-repositorio>
cd Fintech
```

### Paso 2 — Configurar variables de entorno

```bash
# Copia la plantilla
cp .env.example .env
```

Ahora edita el archivo `.env` con un editor de texto. **Las únicas variables OBLIGATORIAS que debes cambiar antes del primer uso** son:

```env
# Genera la Fernet Key ejecutando en tu terminal:
# python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
AIRFLOW_FERNET_KEY=<pega-aquí-tu-fernet-key>

# Genera una secret key:
# python -c "import secrets; print(secrets.token_hex(32))"
AIRFLOW_SECRET_KEY=<pega-aquí-tu-secret-key>

# Cambia la contraseña de la base de datos
POSTGRES_PASSWORD=mi_password_seguro_aqui

# Cambia la contraseña de Grafana
GRAFANA_PASSWORD=mi_password_grafana
```

> 🔐 **NUNCA** pongas `.env` en git. Está en `.gitignore` por defecto.

### Paso 3 — Arrancar el stack completo

```bash
make up
```

Este comando hace lo siguiente automáticamente:
1. Construye todas las imágenes Docker (primera vez tarda ~5-10 min)
2. Levanta los 11 servicios (PostgreSQL, Redis, LocalStack, Airflow, Spark, Streamlit, Grafana, Prometheus…)
3. Espera 60 segundos para que los servicios estén healthy
4. Ejecuta Terraform para crear el bucket S3 en LocalStack

### Paso 4 — Verificar que todo está corriendo

```bash
docker compose ps
```

Deberías ver algo similar a:

```
NAME                        STATUS          PORTS
fintech-postgres            healthy         0.0.0.0:5432->5432/tcp
fintech-redis               healthy         0.0.0.0:6379->6379/tcp
fintech-localstack          healthy         0.0.0.0:4566->4566/tcp
fintech-airflow-webserver   healthy         0.0.0.0:8080->8080/tcp
fintech-airflow-scheduler   healthy
fintech-airflow-worker      running
fintech-spark-master        healthy         0.0.0.0:8081->8081/tcp
fintech-spark-worker        running
fintech-streamlit           running         0.0.0.0:8501->8501/tcp
fintech-prometheus          running         0.0.0.0:9090->9090/tcp
fintech-grafana             running         0.0.0.0:3000->3000/tcp
```

### URLs de los servicios

| Servicio | URL | Usuario | Contraseña |
|---------|-----|---------|------------|
| **Airflow** | http://localhost:8080 | `admin` | `admin` |
| **Streamlit** | http://localhost:8501 | — | — |
| **Spark UI** | http://localhost:8081 | — | — |
| **Grafana** | http://localhost:3000 | `admin` | valor de `GRAFANA_PASSWORD` |
| **Prometheus** | http://localhost:9090 | — | — |

---

## 5. Generación de Datos

### Generar transacciones sintéticas

```bash
make generate
```

Este comando lanza el generador de datos que:
- Crea **100 usuarios** ficticios con diferentes perfiles de riesgo (low/medium/high)
- Crea **50 comerciantes** ficticios con categorías MCC
- Genera **1,000 transacciones** (cantidad configurable en `.env`) con:
  - **~2% de fraude real** inyectado (configurable con `GENERATOR_FRAUD_RATE`)
  - Patrones de fraude: Velocity Attack, High Amount, Geographic Impossible
- Sube todo a LocalStack S3 en formato JSONL bajo `s3://fintech-raw-data/raw/`

### Verificar los datos en S3

```bash
docker exec fintech-localstack \
  awslocal s3 ls s3://fintech-raw-data/raw/transactions/ --recursive
```

### Configurar el volumen de datos

Edita `.env`:
```env
GENERATOR_BATCH_SIZE=5000    # Número de transacciones por lote
GENERATOR_FRAUD_RATE=0.02    # 2% de fraude (0.01 = 1%, 0.05 = 5%)
```

---

## 6. Ejecutar el Pipeline

### Opción A — Automático (recomendado)

Airflow ejecuta el pipeline **automáticamente cada hora**. Solo necesitas:

1. Ir a http://localhost:8080
2. Iniciar sesión con `admin` / `admin`
3. Activar el DAG `fintech_master_pipeline` (toggle azul)
4. Hacer clic en ▶ **Trigger DAG** para ejecutarlo inmediatamente

### Opción B — Manual (para desarrollo/pruebas)

Ejecuta cada capa individualmente:

```bash
# 1. Bronze (leer JSONL → guardar Parquet en S3)
docker exec fintech-spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/bronze_ingest.py \
  --date $(date +%Y-%m-%d) --hour $(date +%H)

# 2. Silver (limpiar + hash PII)
docker exec fintech-spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/silver_cleanse.py \
  --date $(date +%Y-%m-%d)

# 3. Gold (cargar en PostgreSQL)
docker exec fintech-spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/gold_modeling.py \
  --date $(date +%Y-%m-%d)

# 4. Detección de anomalías
docker exec fintech-spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/anomaly_detection.py \
  --date $(date +%Y-%m-%d)

# 5. Transformaciones dbt
make dbt-run
```

### ¿Qué hace cada capa?

#### 🥉 Bronze Layer
- **Entrada:** Archivos JSONL en `s3://fintech-raw-data/raw/transactions/YYYY/MM/DD/HH/`
- **Proceso:** Lee el JSON tal cual, añade metadatos (`batch_id`, `ingestion_timestamp`)
- **Salida:** Parquet en `s3://fintech-raw-data/bronze/transactions/partition_date=YYYY-MM-DD/`
- **Regla:** Sin filtrado. Todos los datos pasan (inmutabilidad).

#### 🥈 Silver Layer
- **Entrada:** Parquet de Bronze
- **Proceso:**
  - Parsea el campo `raw_data` (JSON string) a columnas tipadas
  - Castea tipos: amounts → `DECIMAL(18,2)`, timestamps → `TIMESTAMP`
  - **Elimina PII:** `ip_address` → `ip_address_hash` (SHA-256 de 64 chars)
  - Filtra filas inválidas: nulos en campos clave, amounts ≤ 0, fechas futuras
  - Deduplica por `transaction_id` (se queda con el más reciente)
- **Salida:** Parquet limpio en `s3://fintech-raw-data/silver/transactions/partition_date=YYYY-MM-DD/`

#### 🥇 Gold Layer
- **Entrada:** Parquet de Silver
- **Proceso:**
  - Escribe en tablas de staging en PostgreSQL
  - dbt hace el upsert hacia `gold.dim_users`, `gold.dim_merchants`, `gold.fact_transactions`
  - Mantiene SCD Type 2 para usuarios (historial de cambios)
- **Salida:** Tablas relacionales en PostgreSQL con índices optimizados

#### 🚨 Anomaly Detection
- **Entrada:** Tabla `gold.fact_transactions` (recién cargada)
- **Proceso:** Aplica 3 reglas:

| Regla | Condición | Severidad |
|-------|-----------|-----------|
| `VELOCITY_ATTACK` | ≥ 10 transacciones del mismo usuario en 60 segundos | `min(count/20, 1.0)` |
| `HIGH_AMOUNT` | Monto > 5× el promedio histórico del usuario | `min(ratio, 1.0)` |
| `GEO_IMPOSSIBLE` | Velocidad de viaje implícita > 800 km/h | `min(speed/2000, 1.0)` |

- **Salida:** Registros en `gold.alerts_log` con `reason_code`, `severity_score`, `description`

---

## 7. Dashboard Streamlit

Accede en: **http://localhost:8501**

### Página 1: 📊 Executive KPIs
Muestra los KPIs ejecutivos de los últimos 30 días:
- **TPV (Total Payment Volume):** suma de transacciones completadas en dólares
- **Total Transacciones:** conteo total
- **Valor Promedio:** monto promedio por transacción
- **Fraud Rate %:** porcentaje de transacciones marcadas como fraude
- Gráfico de área de TPV diario
- Pie chart de métodos de pago
- Bar chart de tipos de transacción

### Página 2: 🚨 Fraud Monitoring
Monitoreo en tiempo real de alertas de fraude:
- Filtros por período (7/30/90 días) y estado de alerta
- Métricas de alertas abiertas y severidad promedio
- Gráfico de alertas por tipo de regla
- Distribución de severidad (Critical/High/Medium/Low)
- Línea temporal de alertas por regla
- Tabla interactiva con las últimas 50 alertas

### Página 3: ⚙️ Operational Health
Salud operacional del pipeline:
- Conteo de filas en todas las tablas Gold
- Gauges de SLA (volumen de ingest, fraud rate, alertas abiertas)
- Tasa de rechazo Silver → Gold (últimos 7 días)

> 📌 **Nota:** Los datos se recargan automáticamente cada 2-5 minutos (TTL en caché). Para forzar recarga, usa el botón ↺ en Streamlit.

---

## 8. Monitoreo con Grafana

Accede en: **http://localhost:3000** | Usuario: `admin` | Contraseña: valor de `GRAFANA_PASSWORD` en `.env`

### Dashboards disponibles (auto-provisionados)

1. **Fintech Executive KPIs**
   - Total Payment Volume (stat card)
   - Fraud Rate % (stat card con thresholds rojo/amarillo/verde)
   - Total Transacciones (stat card)
   - Open Fraud Alerts (stat card)
   - Gráfico de TPV diario (timeseries 30 días)
   - Alertas por tipo de regla (bar chart)

2. **Fintech Fraud Monitoring**
   - Open Alerts (contador con thresholds)
   - Average Severity Score (gauge 0-1)
   - Alert Timeline by Rule (timeseries 7 días)
   - Alerts by Country Code (bar chart top 10)

### Alertas automáticas configuradas en Prometheus

| Alerta | Condición | Severidad |
|--------|-----------|-----------|
| `LowIngestVolume` | < 100 transacciones en la última hora | warning |
| `FraudRateSpike` | Fraud rate > 10% en la última hora | critical |
| `OpenAlertsBacklog` | > 50 alertas abiertas por más de 24 horas | warning |

---

## 9. Airflow — Orquestación

Accede en: **http://localhost:8080** | Usuario: `admin` | Contraseña: `admin`

### DAG: `fintech_master_pipeline`

```
[generate_data]
      ↓
[wait_for_s3_data]  ← S3 Sensor (espera hasta 30 min)
      ↓
[bronze_ingest]     ← Spark job
      ↓
[silver_cleanse]    ← Spark job
      ↓
[ge_validate]       ← Great Expectations (15+ validaciones)
      ↓
   ┌──┴──┐
[gold_modeling]  [anomaly_detection]  ← Paralelo
   └──┬──┘
      ↓
[ml_scoring]        ← Isolation Forest (stretch goal)
      ↓
[dbt_run]           ← Modelos marts
      ↓
[dbt_test]          ← Tests de calidad
```

### Programación: cada hora (`@hourly`)

### Cómo monitorear una ejecución
1. En la página principal, click en `fintech_master_pipeline`
2. Click en **Graph** para ver el estado de cada tarea
3. Click en cualquier tarea → **Log** para ver los logs detallados

### Roles de acceso Airflow
| Rol | Permisos |
|-----|---------|
| `Admin` | Todo (para el equipo de datos) |
| `User` | Disparar DAGs, ver logs |
| `Op` | Gestionar task instances |
| `Viewer` | Solo lectura |

---

## 10. dbt — Transformaciones SQL

dbt corre dentro del contenedor de Airflow. Para ejecutarlo manualmente:

```bash
# Ejecutar todos los modelos
make dbt-run

# Ejecutar solo los modelos de staging
docker compose exec airflow-scheduler bash -c \
  "cd /opt/dbt && dbt run --select staging --target dev"

# Ejecutar tests de calidad
make dbt-test

# Ver documentación generada
docker compose exec airflow-scheduler bash -c \
  "cd /opt/dbt && dbt docs generate && dbt docs serve --port 8090"
# Luego abre http://localhost:8090
```

### Modelos disponibles

| Modelo | Tipo | Descripción |
|--------|------|-------------|
| `stg_transactions` | view | Normalización de staging.fact_transactions_staging |
| `stg_users` | view | Normalización de staging.dim_users_staging |
| `stg_merchants` | view | Normalización de staging.dim_merchants_staging |
| `dim_users` | table | Dimensión usuarios (con SCD2 via snapshot) |
| `dim_merchants` | table | Dimensión comerciantes |
| `fct_transactions` | incremental | Tabla de hechos principal |
| `mart_fraud_summary` | table | Resumen diario de fraude por país |
| `mart_executive_kpis` | table | KPIs diarios: TPV, fraud rate, avg value |

### Tests de calidad ejecutados automáticamente

| Test | Qué valida |
|------|-----------|
| `assert_positive_revenue` | Ninguna transacción en Gold tiene amount ≤ 0 |
| `assert_user_uniqueness` | No hay dos registros `is_current=TRUE` para el mismo usuario |
| `assert_no_orphan_facts` | Todas las transacciones tienen un usuario válido |
| `assert_fraud_score_range` | `fraud_score` está en el rango [0, 1] |
| Schema tests | `not_null`, `unique`, `accepted_values` en todos los modelos |

---

## 11. Pruebas

### Tests unitarios (no requieren Docker)

```bash
# Instala las dependencias de processing
cd processing && pip install -r requirements.txt

# Ejecuta todos los tests unitarios
pytest processing/tests/ -v

# Tests específicos
pytest processing/tests/test_silver_cleanse.py -v
pytest processing/tests/test_anomaly_rules.py -v
pytest processing/tests/test_ml_scoring.py -v
```

### Tests de integración (requieren stack corriendo)

```bash
# Asegúrate de que el stack esté up y datos generados
make up
make generate

# Ejecuta tests de integración
make test-integration
```

Los tests de integración validan:
- Bucket S3 `fintech-raw-data` existe
- Archivos raw existen en S3 (tras `make generate`)
- Schemas `gold` y `staging` existen en PostgreSQL
- `dim_time` tiene ≥ 3,000 filas (2020–2030)
- `fact_transactions` tiene datos (tras pipeline)
- No existe columna `ip_address` en Gold (validación PII)
- `alerts_log` tiene las 3 reglas de fraude

### Tests de performance (SLA queries)

```bash
make test-perf
```

Valida que las consultas del dashboard completan en < 2 segundos (p95 SLA).

### Cobertura de código

```bash
pytest --cov=processing --cov-report=html:coverage_report
# Luego abre coverage_report/index.html
```

Objetivo: ≥ 70% de cobertura de código (forzado en `pytest.ini`).

---

## 12. Comandos Make de Referencia

```bash
make help              # Muestra todos los comandos disponibles

# ── Stack ──
make up                # Iniciar todo el stack (primera vez ~10 min)
make down              # Detener y destruir todo (incluye volúmenes)
make clean             # Limpieza total (imágenes, volúmenes, pycache)
make ps                # Ver estado de contenedores

# ── Datos y pipeline ──
make generate          # Generar datos sintéticos y subir a S3
make infra             # Re-provisionar infraestructura Terraform
make dbt-run           # Ejecutar modelos dbt
make dbt-test          # Ejecutar tests de calidad dbt

# ── Logs ──
make logs              # Tail de todos los logs
make logs-airflow      # Solo logs del scheduler de Airflow
make logs-spark        # Solo logs de Spark master

# ── Tests ──
make test              # Tests unitarios
make test-integration  # Tests de integración (stack requerido)
make test-perf         # Tests de performance SLA
make test-fraud        # Benchmark de detección de fraude
make test-dash         # Tests de latencia de dashboard

# ── Calidad de código ──
make lint              # flake8 + mypy en processing/
```

---

## 13. Solución de Problemas Comunes

### ❌ `make up` falla con "Address already in use"

Algún puerto está ocupado. Identifica cuál:
```bash
# Verifica qué proceso usa el puerto 8080
netstat -ano | findstr :8080   # Windows
lsof -i :8080                  # Mac/Linux
```

Modifica el puerto en `docker-compose.yml` (lado izquierdo del mapeo).

---

### ❌ Airflow no aparece en http://localhost:8080

```bash
# Verifica logs del webserver
docker compose logs airflow-webserver --tail 50

# Verifica logs del init
docker compose logs airflow-init --tail 30

# Si hay error de DB migration, re-inicializa manualmente
make airflow-init
```

---

### ❌ `make generate` falla con "Connection refused" o "Bucket does not exist"

LocalStack no está listo o el bucket no fue creado:
```bash
# 1. Verifica que LocalStack esté healthy
curl http://localhost:4566/_localstack/health

# 2. Re-aprovisiona la infraestructura
make infra

# 3. Vuelve a intentar
make generate
```

---

### ❌ Spark job falla con "ClassNotFoundException" o "NoClassDefFoundError"

Falta el JAR de Hadoop/AWS. Verifica la configuración de `spark.jars.packages` en `processing/utils/spark_session.py`. En la primera ejecución, Spark descarga los JARs (~500 MB). Necesitas conexión a internet.

---

### ❌ Streamlit muestra "No data yet"

El pipeline no ha corrido o los datos no llegaron a Gold:
```bash
# Verifica conteo en Gold
docker exec fintech-postgres \
  psql -U fintech_user -d fintech_dw -c \
  "SELECT COUNT(*) FROM gold.fact_transactions;"

# Si es 0, ejecuta:
make generate
# Luego dispara el DAG desde Airflow UI, o ejecuta dbt:
make dbt-run
```

---

### ❌ Error de Fernet Key "Invalid key"

La Fernet Key en `.env` no es válida. Genera una nueva:
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```
Pega el resultado en `AIRFLOW_FERNET_KEY=` en tu `.env` y reinicia:
```bash
make down && make up
```

---

### ❌ Grafana muestra "datasource not found"

```bash
# Reinicia Grafana para recargar el provisioning
docker compose restart grafana

# Verifica que las variables de entorno están en .env
cat .env | grep POSTGRES
```

---

### 🔄 Reset completo (desde cero)

```bash
make clean   # Elimina todo
# Edita .env con credenciales correctas
make up      # Reinicia
make generate
```

---

## 14. Resumen Visual del Flujo

```
┌─────────────────────────────────────────────────────────────┐
│                     FLUJO COMPLETO                           │
│                                                               │
│  1. make up          → Stack listo (~10 min)                 │
│  2. make generate    → Datos en S3 (~2 min)                  │
│                                                               │
│  3. Airflow (auto-cada hora o manual):                        │
│     ┌──────────────────────────────────┐                     │
│     │  bronze_ingest.py  (~1 min)      │                     │
│     │       ↓                          │                     │
│     │  silver_cleanse.py (~2 min)      │                     │
│     │       ↓                          │                     │
│     │  great_expectations (~30s)       │                     │
│     │       ↓                          │                     │
│     │  gold_modeling.py  (~2 min)      │                     │
│     │  anomaly_detection (~1 min)      │ ← paralelo           │
│     │       ↓                          │                     │
│     │  ml_scoring.py     (~2 min)      │                     │
│     │       ↓                          │                     │
│     │  dbt run + test    (~3 min)      │                     │
│     └──────────────────────────────────┘                     │
│                                                               │
│  4. Dashboard Streamlit → http://localhost:8501              │
│  5. Grafana            → http://localhost:3000              │
│                                                               │
│  Total pipeline end-to-end: ~12 min                          │
└─────────────────────────────────────────────────────────────┘
```

---

## 📞 Soporte

Para reportar errores o hacer preguntas, revisa primero:
1. `docs/RUNBOOK.md` — Guía de operaciones detallada
2. `docs/ARCHITECTURE.md` — Diagramas de arquitectura
3. `docs/DATA_CONTRACTS.md` — Esquemas de datos
4. `docs/TECH_STACK.md` — Decisiones de tecnología

---

*Fintech Data Intelligence Platform © 2026 — Proyecto académico / portafolio*

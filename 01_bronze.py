# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Plataforma de Auditoría Fiscal B2B · 01 · Ingesta Incremental Bronze
# MAGIC 
# MAGIC Este notebook orquesta la ingesta continua y escalable de facturas electrónicas hacia la capa Bronze gobernada mediante **Databricks Auto Loader** (`cloudFiles`).
# MAGIC 
# MAGIC ### 🎯 Principios de Diseño:
# MAGIC 1. **Ingesta Orientada a Flujos Continuos:** Detección automática y carga eficiente de archivos semiestructurados (JSON) desde el Volume de aterrizaje.
# MAGIC 2. **Inferencia y Evolución Segura de Esquema:** Empleo de `schemaLocation` para adaptar cambios de estructura sin interrumpir el servicio.
# MAGIC 3. **Trazabilidad y Linaje:** Inclusión de metadatos del archivo de origen (`_metadata.file_path`) y marca temporal de ingestión.
# MAGIC 4. **Procesamiento Idempotente (Exactly-Once):** Garantía estricta de no duplicación de registros ante ejecuciones sucesivas.

# COMMAND ----------

from pyspark.sql import functions as F

origen = "/Volumes/pf_avalos/bronze/landing/facturas/"
formato = "json"
tabla_bronze = "pf_avalos.bronze.facturas_raw"
checkpoint = "/Volumes/pf_avalos/bronze/landing/_checkpoints/facturas_raw"

# COMMAND ----------

# Pipeline de Ingesta Incremental con Auto Loader
(spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", formato)
    .option("cloudFiles.schemaLocation", checkpoint)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("pathGlobFilter", f"*.{formato}")
    .load(origen)
    .withColumn("archivo_origen", F.col("_metadata.file_path"))
    .withColumn("fecha_ingesta", F.current_timestamp())
    .writeStream
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .toTable(tabla_bronze)
    .awaitTermination())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Evidencia de Ingesta Incremental
# MAGIC Conteo de transacciones agrupadas por lote físico de llegada. Cada lote debe figurar una sola vez con su respectiva marca temporal.

# COMMAND ----------

display(spark.sql(f"""
  SELECT regexp_extract(archivo_origen, 'lote_[0-9]+', 0) AS lote,
         count(*) AS total_facturas_ingestadas,
         min(fecha_ingesta) AS hora_inicio_ingesta,
         max(fecha_ingesta) AS hora_fin_ingesta
  FROM {tabla_bronze}
  GROUP BY 1 
  ORDER BY 1
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📋 Justificación Técnica de Arquitectura: Checkpoints e Idempotencia
# MAGIC 
# MAGIC **Evaluación de escenario:** *¿Qué ocurre al ejecutar esta ingesta de forma consecutiva sin llegada de archivos nuevos?*
# MAGIC 
# MAGIC **Decisión de Ingeniería:**
# MAGIC - **Comportamiento Operativo:** Auto Loader evalúa la ruta física en el Volume contra el registro de estado del stream. Al detectar 0 archivos pendientes, la ejecución con `trigger(availableNow=True)` finaliza exitosamente en milisegundos sin insertar filas vacías ni duplicados en `pf_avalos.bronze.facturas_raw`. La operación es **100% idempotente**.
# MAGIC - **Rol del Checkpoint:** En el directorio `_checkpoints`, Spark mantiene:
# MAGIC   - **`offsets/`**: Bitácora cronológica con los identificadores de los archivos ya indexados.
# MAGIC   - **`commits/`**: Registro atómico de las micro-tandas confirmadas en la tabla Delta, garantizando semántica de procesamiento *exactly-once*.
# MAGIC   - **`_schemas/`**: Esquema inferido persistido, lo que evita escaneos de inferencia repetitivos y gestiona la evolución del esquema en caliente.

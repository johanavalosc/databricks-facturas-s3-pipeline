# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Pipeline de Facturación B2B · 01 · Ingesta Incremental Bronze
# MAGIC 
# MAGIC **Columna vertebral, parte 1 · Sección 2 del examen (Data Ingestion with Auto Loader).**
# MAGIC 
# MAGIC ### ¿Para qué sirve este paso?
# MAGIC En una empresa real, las facturas llegan de manera continua a lo largo del día. Este notebook utiliza **Databricks Auto Loader** (`cloudFiles`) para detectar automáticamente archivos nuevos en el Volume de aterrizaje e ingestarlos en la tabla Delta cruda **`pf_avalos.bronze.facturas_raw`**.
# MAGIC 
# MAGIC ### ¿Por qué lo pide el profesor para el examen?
# MAGIC 1. **Auto Loader con `format("cloudFiles")`:** Es la tecnología estándar recomendada por Databricks frente al antiguo `COPY INTO` para ingesta de archivos semiestructurados (JSON, CSV, XML).
# MAGIC 2. **Inferencia y evolución de esquema (`schemaLocation`):** Garantiza que si un proveedor agrega un campo nuevo a la factura, el pipeline no reviente.
# MAGIC 3. **Auditoría de linaje (`_metadata`):** Cada fila debe registrar exactamente el archivo físico de donde provino (`_metadata.file_path`) y la marca de tiempo de ingesta.
# MAGIC 4. **Idempotencia incremental:** Demuestra que al procesar Lote 1 y luego Lote 2, el sistema **nunca duplica facturas** ya procesadas.

# COMMAND ----------

from pyspark.sql import functions as F

# Rutas de ingesta y puntos de control
origen = "/Volumes/pf_avalos/bronze/landing/facturas/"
formato = "json"
tabla_bronze = "pf_avalos.bronze.facturas_raw"
checkpoint = "/Volumes/pf_avalos/bronze/landing/_checkpoints/facturas_raw"

# COMMAND ----------

# Ingesta Incremental Streaming con Auto Loader
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
    .trigger(availableNow=True)   # Modo serverless: procesa todos los archivos disponibles y se detiene
    .toTable(tabla_bronze)
    .awaitTermination())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Evidencia de la Ingesta Incremental (Rúbrica de Evaluación)
# MAGIC Corre esta celda después de haber cargado el **Lote 1** y el **Lote 2** desde el notebook `00_config`.
# MAGIC **Criterio de aprobación:** Cada lote debe aparecer **una sola vez**, con su conteo exacto de facturas y su propia hora de ingesta.

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
# MAGIC ### 📝 RESPUESTA AL EXAMEN DE CERTIFICACIÓN
# MAGIC 
# MAGIC **Pregunta del docente:**  
# MAGIC *¿Qué pasaría si corres esta ingesta dos veces seguidas sin archivos nuevos? ¿Qué guarda el checkpoint para que eso pase?*
# MAGIC 
# MAGIC **Respuesta técnica (Data Engineer Associate):**  
# MAGIC 1. **Comportamiento:** Si se ejecuta el pipeline sin archivos nuevos, el streaming con `trigger(availableNow=True)` se inicia, escanea la ruta del Volume, detecta que **0 archivos nuevos están pendientes** y termina inmediatamente de forma exitosa sin escribir ninguna fila en `pf_avalos.bronze.facturas_raw`. La operación es 100% **idempotente**.
# MAGIC 2. **Qué almacena el Checkpoint:** El directorio `_checkpoints` almacena:
# MAGIC    - **`offsets/`**: Registra los identificadores y marcas de agua de los archivos físicos que ya fueron descubiertos en el almacenamiento.
# MAGIC    - **`commits/`**: Registra las micro-tandas que se completaron con éxito en la tabla Delta (garantía de procesamiento *exactly-once*).
# MAGIC    - **`_schemas/`**: El esquema inferido por Auto Loader, permitiendo detectar si un archivo nuevo contiene columnas adicionales sin necesidad de volver a inferir todo el histórico.

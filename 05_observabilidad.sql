-- Databricks notebook source
-- MAGIC %md
-- MAGIC # ⚡ Pipeline de Facturación B2B · 05 · Observabilidad y Optimización
-- MAGIC 
-- MAGIC **Componente 5 del examen · Secciones 6 y 7 (Optimization, Performance, and System Tables Monitoring).**
-- MAGIC 
-- MAGIC ### ¿Para qué sirve este paso?
-- MAGIC Un Data Engineer no solo escribe pipelines, sino que garantiza que las consultas vuelen en producción con bajo consumo de DBUs y monitorea la salud del sistema.
-- MAGIC Este notebook implementa:
-- MAGIC 1. **Liquid Clustering (`CLUSTER BY`):** La técnica de optimización de última generación de Delta Lake que reemplaza al particionado rígido tradicional.
-- MAGIC 2. **Compactación Física (`OPTIMIZE`):** Reorganización de micro-archivos Parquet para maximizar el *Data Skipping*.
-- MAGIC 3. **Inspección de Metadatos:** Verificación con `DESCRIBE DETAIL` y `DESCRIBE HISTORY`.
-- MAGIC 4. **Telemetría con System Tables:** Consultas de auditoría, linaje y tiempos de ejecución del Lakeflow Job.

-- COMMAND ----------

USE CATALOG pf_avalos;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Optimización con Liquid Clustering
-- MAGIC Aplicamos Liquid Clustering sobre la tabla central **`silver.facturas_validadas`** agrupando por **`periodo_fiscal`** y **`categoria_gasto`**.

-- COMMAND ----------

-- Configurar Liquid Clustering en la tabla principal
ALTER TABLE silver.facturas_validadas 
  CLUSTER BY (periodo_fiscal, categoria_gasto);

-- Ejecutar la compactación física de archivos
OPTIMIZE silver.facturas_validadas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 🔍 Evidencia 1: Inspección de Detalles de la Tabla (DESCRIBE DETAIL)
-- MAGIC Verifica que en la columna `clusteringColumns` aparezcan `["periodo_fiscal", "categoria_gasto"]`.

-- COMMAND ----------

DESCRIBE DETAIL silver.facturas_validadas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 🔍 Evidencia 2: Historial de Transacciones Delta (DESCRIBE HISTORY)
-- MAGIC Demuestra la ejecución atómica del comando `OPTIMIZE` y la compactación de archivos en el log Delta.

-- COMMAND ----------

DESCRIBE HISTORY silver.facturas_validadas
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Consultas a las Tablas del Sistema (System Tables)
-- MAGIC *Nota del docente: Las tablas del sistema pueden tener una pequeña latencia de sincronización (auditoría unos minutos, linaje ~20 min).*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### A. Auditoría: ¿Quién creó o modificó objetos en tu catálogo recientemente?

-- COMMAND ----------

SELECT 
  event_time,
  user_identity.email AS usuario,
  action_name,
  coalesce(request_params['full_name_arg'], request_params['name']) AS objeto_modificado,
  response['status_code'] AS codigo_respuesta
FROM system.access.audit
WHERE event_date >= current_date() - INTERVAL 7 DAYS
  AND lower(to_json(request_params)) LIKE '%pf_avalos%'
ORDER BY event_time DESC
LIMIT 25;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B. Linaje: Trazabilidad de tablas de Bronze a Gold

-- COMMAND ----------

SELECT DISTINCT 
  source_table_full_name,
  target_table_full_name,
  entity_type
FROM system.access.table_lineage
WHERE target_table_full_name LIKE 'pf_avalos.%'
  AND event_date >= current_date() - INTERVAL 7 DAYS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C. Telemetría de Jobs: Tiempos y estado de ejecución del Lakeflow Job

-- COMMAND ----------

SELECT 
  period_start_time,
  job_name,
  run_id,
  task_key,
  execution_state,
  termination_code
FROM system.lakeflow.job_run_timeline
WHERE event_date >= current_date() - INTERVAL 7 DAYS
ORDER BY period_start_time DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 📝 RESPUESTA AL EXAMEN DE CERTIFICACIÓN
-- MAGIC 
-- MAGIC **Pregunta del docente:**  
-- MAGIC *¿Por qué elegiste esas columnas para el clustering? ¿Qué consulta de tu proyecto se beneficia?*
-- MAGIC 
-- MAGIC **Respuesta técnica (Data Engineer Associate):**  
-- MAGIC 1. **Columnas elegidas:** `(periodo_fiscal, categoria_gasto)`.
-- MAGIC    - **`periodo_fiscal`:** Tiene una granularidad temporal perfecta (mes a mes) con alta cardinalidad controlada. La totalidad de las consultas financieras y cierres contables filtran obligatoriamente por mes fiscal (`WHERE periodo_fiscal = '2026-10'`).
-- MAGIC    - **`categoria_gasto`:** Es la dimensión principal de análisis de compras utilizada en las cláusulas `GROUP BY` y en los filtros de reportería para auditoría de proveedores.
-- MAGIC 2. **Por qué Liquid Clustering frente al particionado tradicional:**  
-- MAGIC    - El particionado tradicional de directorios Hive (`/periodo_fiscal=2026-10/`) crea el problema de micro-archivos (*small files problem*) cuando los lotes son pequeños.
-- MAGIC    - Liquid Clustering utiliza curvas de Hilbert para co-localizar físicamente los datos multidimensionales en los mismos archivos Parquet, permitiendo a Spark realizar **Data Skipping masivo** leyendo únicamente las estadísticas de los archivos relevantes sin escanear la tabla completa.

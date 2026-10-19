-- Databricks notebook source
-- MAGIC %md
-- MAGIC # ⚡ Plataforma de Auditoría Fiscal B2B · 05 · Optimización y Observabilidad
-- MAGIC 
-- MAGIC Este notebook implementa técnicas avanzadas de aceleración de consultas mediante **Liquid Clustering** y supervisa la actividad operacional y de linaje mediante **System Tables**.
-- MAGIC 
-- MAGIC ### 🎯 Alcance de Rendimiento y Monitoreo:
-- MAGIC 1. **Liquid Clustering (`CLUSTER BY`):** Reorganización multidimensional de datos en Delta Lake sin las restricciones del particionado tradicional.
-- MAGIC 2. **Compactación Física (`OPTIMIZE`):** Unificación de micro-archivos para maximizar el *Data Skipping*.
-- MAGIC 3. **Auditoría de Acceso:** Inspección de eventos sobre el catálogo en `system.access.audit`.
-- MAGIC 4. **Trazabilidad de Linaje:** Seguimiento de transformaciones en `system.access.table_lineage`.

-- COMMAND ----------

USE CATALOG pf_avalos;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Optimización con Liquid Clustering
-- MAGIC Configuración de agrupamiento multidimensional en `silver.facturas_validadas` por `periodo_fiscal` y `categoria_gasto`.

-- COMMAND ----------

ALTER TABLE silver.facturas_validadas 
  CLUSTER BY (periodo_fiscal, categoria_gasto);

OPTIMIZE silver.facturas_validadas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 🔍 Inspección de Metadatos de Tabla (DESCRIBE DETAIL)
-- MAGIC Verificación de las columnas de clustering activas en la definición física.

-- COMMAND ----------

DESCRIBE DETAIL silver.facturas_validadas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 🔍 Bitácora Transaccional Delta (DESCRIBE HISTORY)
-- MAGIC Registro atómico de la operación de compactación `OPTIMIZE`.

-- COMMAND ----------

DESCRIBE HISTORY silver.facturas_validadas
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Monitoreo y Telemetría en Tablas del Sistema (System Tables)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### A. Eventos Recientes de Creación y Modificación en el Catálogo

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
-- MAGIC ### B. Linaje de Datos entre Capas Medallion

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
-- MAGIC ### C. Tiempos y Estado de Ejecución de Tareas en Lakeflow

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
-- MAGIC ### 📋 Justificación Técnica: Liquid Clustering vs. Particionado Hive
-- MAGIC 
-- MAGIC **Decisiones de Rendimiento:**
-- MAGIC 1. **Columnas de Agrupamiento:** `(periodo_fiscal, categoria_gasto)`.
-- MAGIC    - Las consultas analíticas de auditoría y reportes para dirección financiera filtran obligatoriamente por rango mensual (`periodo_fiscal = '2026-10'`) y desglosan por centro de costo o rubro de insumo (`categoria_gasto`).
-- MAGIC 2. **Ventajas frente al particionado tradicional:**  
-- MAGIC    - El particionado rígido basado en carpetas de directorios Hive genera dispersión en micro-archivos (*small files*) cuando los lotes de facturación son frecuentes.
-- MAGIC    - Liquid Clustering organiza físicamente las filas contiguas mediante algoritmos de Hilbert Curve dentro de los archivos Parquet, logrando un **Data Skipping** bidimensional sumamente veloz sin fragmentar el almacenamiento.

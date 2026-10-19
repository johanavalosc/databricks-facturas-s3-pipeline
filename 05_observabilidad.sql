-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Proyecto final · 05 · Observabilidad y optimización
-- MAGIC 
-- MAGIC **Componente 5 · Secciones 6 y 7 del examen.**
-- MAGIC 
-- MAGIC Lo que se evalúa:
-- MAGIC - Una consulta a las tablas del sistema sobre **tu** proyecto: auditoría, linaje o corridas del job.
-- MAGIC - Liquid clustering en tu tabla más grande, con `OPTIMIZE` y la evidencia en `DESCRIBE DETAIL` o `DESCRIBE HISTORY`.

-- COMMAND ----------

USE CATALOG pf_avalos;

-- COMMAND ----------

SELECT event_time, action_name,
       coalesce(request_params['full_name_arg'], request_params['name']) AS objeto
FROM system.access.audit
WHERE event_date >= current_date() - INTERVAL 7 DAYS
  AND lower(to_json(request_params)) LIKE '%pf_avalos%'
ORDER BY event_time DESC
LIMIT 25;

-- COMMAND ----------

SELECT DISTINCT source_table_full_name, target_table_full_name, entity_type
FROM system.access.table_lineage
WHERE target_table_full_name LIKE 'pf_avalos.%'
  AND event_date >= current_date() - INTERVAL 7 DAYS;

-- COMMAND ----------

SELECT period_start_time, job_name, run_id, task_key, execution_state
FROM system.lakeflow.job_run_timeline
WHERE event_date >= current_date() - INTERVAL 7 DAYS
ORDER BY period_start_time DESC
LIMIT 20;

-- COMMAND ----------

ALTER TABLE pf_avalos.silver.facturas_validadas CLUSTER BY (periodo_fiscal, categoria_gasto);
OPTIMIZE pf_avalos.silver.facturas_validadas;

-- COMMAND ----------

DESCRIBE DETAIL pf_avalos.silver.facturas_validadas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### TU RESPUESTA
-- MAGIC ¿Por qué elegiste esa columna para el clustering? ¿Qué consulta de tu proyecto se beneficia?
-- MAGIC 
-- MAGIC Elegí `(periodo_fiscal, categoria_gasto)` porque los reportes analíticos de compras y auditorías fiscales siempre filtran obligatoriamente por mes y agrupan por rubro de insumo. Liquid Clustering co-localiza los datos físicamente en los archivos Parquet, permitiendo Data Skipping acelerado sin fragmentar el almacenamiento en micro-archivos.

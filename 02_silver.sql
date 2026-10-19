-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Proyecto final · 02 · Silver
-- MAGIC 
-- MAGIC **Columna vertebral, parte 2 · Sección 3 del examen.** La tabla silver es la versión limpia de bronze.
-- MAGIC 
-- MAGIC Lo que se evalúa:
-- MAGIC - Tipos correctos (fechas como fecha, montos como número).
-- MAGIC - Nulos tratados con una regla que puedas explicar.
-- MAGIC - Deduplicación por la llave del negocio.
-- MAGIC - Al menos un join que enriquezca la tabla.
-- MAGIC - Una celda de métricas antes y después.

-- COMMAND ----------

USE CATALOG pf_avalos;

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.facturas_validadas AS
WITH facturas_dedup AS (
  SELECT *
  FROM bronze.facturas_raw
  WHERE clave_hacienda IS NOT NULL AND monto_total >= 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY clave_hacienda ORDER BY fecha_ingesta DESC) = 1
)
SELECT
  d.clave_hacienda,
  d.consecutivo,
  to_timestamp(d.fecha_emision) AS fecha_emision,
  to_date(d.fecha_emision) AS fecha_emision_date,
  date_format(to_date(d.fecha_emision), 'yyyy-MM') AS periodo_fiscal,
  d.proveedor_id,
  p.razon_social AS proveedor_nombre,
  p.cedula_juridica AS proveedor_cedula,
  p.categoria_gasto,
  coalesce(d.departamento, 'Operaciones') AS departamento,
  coalesce(d.concepto, 'Insumos') AS concepto,
  coalesce(d.moneda, 'CRC') AS moneda,
  coalesce(cast(d.tipo_cambio AS DOUBLE), 1.0) AS tipo_cambio,
  round(CASE WHEN d.moneda = 'USD' THEN d.monto_total * d.tipo_cambio ELSE d.monto_total END, 2) AS total_crc,
  round(CASE WHEN p.es_deducible_iva THEN (CASE WHEN d.moneda = 'USD' THEN d.monto_iva * d.tipo_cambio ELSE d.monto_iva END) ELSE 0.0 END, 2) AS iva_credito_fiscal_crc,
  d.archivo_origen,
  d.fecha_ingesta
FROM facturas_dedup d
INNER JOIN silver.cat_proveedores p ON d.proveedor_id = p.proveedor_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Métricas antes y después

-- COMMAND ----------

SELECT 'bronze' AS capa, count(*) AS filas, count(DISTINCT clave_hacienda) AS llaves_distintas
FROM bronze.facturas_raw
UNION ALL
SELECT 'silver', count(*), count(DISTINCT clave_hacienda)
FROM silver.facturas_validadas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### TU RESPUESTA
-- MAGIC ¿Qué regla usaste para los nulos y los duplicados, y por qué esa y no otra?
-- MAGIC 
-- MAGIC Para duplicados usé `QUALIFY ROW_NUMBER() OVER (PARTITION BY clave_hacienda ORDER BY fecha_ingesta DESC) = 1` porque la clave de Hacienda es la llave tributaria irrepetible de 50 dígitos, asegurando la versión más reciente. Para nulos, descarté comprobantes sin clave o sin monto total porque contablemente no son válidos, y apliqué `coalesce()` en moneda ('CRC') y departamento ('Operaciones').

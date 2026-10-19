-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 🥈 Plataforma de Auditoría Fiscal B2B · 02 · Transformación y Calidad Silver
-- MAGIC 
-- MAGIC Este notebook procesa la capa intermedia (**Silver**), encargada de la homologación, tipado estricto, saneamiento y enriquecimiento relacional de las compras.
-- MAGIC 
-- MAGIC ### 🎯 Objetivos de Transformación:
-- MAGIC 1. **Tipado Estricto de Datos:** Conversión de campos a timestamps, fechas de período fiscal (`yyyy-MM`) y valores numéricos de precisión.
-- MAGIC 2. **Normalización Monetaria Unificada:** Conversión de montos en divisa extranjera (USD) a colones (**CRC**) con base en el tipo de cambio oficial de la transacción.
-- MAGIC 3. **Integridad y Deduplicación Determinista:** Deduplicación estricta por la clave tributaria única del comprobante (`clave_hacienda`).
-- MAGIC 4. **Enriquecimiento Relacional (JOIN):** Cruce con el catálogo corporativo de proveedores (`silver.cat_proveedores`) para auditar la deducibilidad del IVA.
-- MAGIC 5. **Métricas de Control de Calidad:** Celda comparativa de volumen y saneamiento antes vs. después.

-- COMMAND ----------

USE CATALOG pf_avalos;

-- COMMAND ----------

-- Transformación, Enriquecimiento y Saneamiento en Silver
CREATE OR REPLACE TABLE silver.facturas_validadas AS
WITH facturas_tipadas AS (
  SELECT
    trim(f.clave_hacienda) AS clave_hacienda,
    trim(f.consecutivo) AS consecutivo,
    to_timestamp(f.fecha_emision) AS fecha_emision,
    to_date(f.fecha_emision) AS fecha_emision_date,
    date_format(to_date(f.fecha_emision), 'yyyy-MM') AS periodo_fiscal,
    f.proveedor_id,
    coalesce(f.departamento, 'Operaciones') AS departamento,
    coalesce(f.concepto, 'Compra General de Insumos') AS concepto,
    upper(coalesce(f.moneda, 'CRC')) AS moneda,
    coalesce(cast(f.tipo_cambio AS DOUBLE), 1.0) AS tipo_cambio,
    cast(f.monto_subtotal AS DOUBLE) AS monto_subtotal_orig,
    cast(f.monto_iva AS DOUBLE) AS monto_iva_orig,
    cast(f.monto_total AS DOUBLE) AS monto_total_orig,
    upper(coalesce(f.condicion_venta, 'CONTADO')) AS condicion_venta,
    f.archivo_origen,
    f.fecha_ingesta
  FROM bronze.facturas_raw f
  -- Regla de integridad fiscal: descartar registros sin clave tributaria o con monto negativo inicial
  WHERE f.clave_hacienda IS NOT NULL 
    AND f.monto_total IS NOT NULL
    AND f.monto_total >= 0
),
facturas_deduplicadas AS (
  -- Deduplicación por llave natural de negocio conservando la última versión ingestada
  SELECT *
  FROM facturas_tipadas
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY clave_hacienda 
    ORDER BY fecha_ingesta DESC
  ) = 1
)
-- Cruce con catálogo dimensional de proveedores
SELECT
  d.clave_hacienda,
  d.consecutivo,
  d.fecha_emision,
  d.fecha_emision_date,
  d.periodo_fiscal,
  d.proveedor_id,
  p.razon_social AS proveedor_nombre,
  p.cedula_juridica AS proveedor_cedula,
  p.categoria_gasto,
  d.departamento,
  d.concepto,
  d.moneda,
  d.tipo_cambio,
  -- Normalización cambiaria a moneda base (CRC)
  round(CASE WHEN d.moneda = 'USD' THEN d.monto_subtotal_orig * d.tipo_cambio ELSE d.monto_subtotal_orig END, 2) AS subtotal_crc,
  round(CASE WHEN d.moneda = 'USD' THEN d.monto_iva_orig * d.tipo_cambio ELSE d.monto_iva_orig END, 2) AS iva_crc,
  round(CASE WHEN d.moneda = 'USD' THEN d.monto_total_orig * d.tipo_cambio ELSE d.monto_total_orig END, 2) AS total_crc,
  -- Cálculo de Crédito Fiscal: condicionado a la deducibilidad legal del proveedor
  round(CASE 
    WHEN p.es_deducible_iva = true THEN (CASE WHEN d.moneda = 'USD' THEN d.monto_iva_orig * d.tipo_cambio ELSE d.monto_iva_orig END)
    ELSE 0.0 
  END, 2) AS iva_credito_fiscal_crc,
  d.condicion_venta,
  p.plazo_pago_dias,
  d.archivo_origen,
  d.fecha_ingesta,
  current_timestamp() AS fecha_procesamiento_silver
FROM facturas_deduplicadas d
INNER JOIN silver.cat_proveedores p 
  ON d.proveedor_id = p.proveedor_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 📊 Métricas Comparativas de Calidad (Bronze vs. Silver)
-- MAGIC Validación cuantitativa del proceso de depuración, comprobando la eliminación de duplicados y la consistencia en el total monetario.

-- COMMAND ----------

SELECT 
  'bronze (raw)' AS capa,
  count(*) AS total_filas,
  count(DISTINCT clave_hacienda) AS claves_unicas,
  count(CASE WHEN clave_hacienda IS NULL THEN 1 END) AS nulos_en_llave,
  sum(monto_total) AS total_nominal_crudo
FROM bronze.facturas_raw
UNION ALL
SELECT 
  'silver (homologada)',
  count(*),
  count(DISTINCT clave_hacienda),
  count(CASE WHEN clave_hacienda IS NULL THEN 1 END),
  sum(total_crc)
FROM silver.facturas_validadas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 📋 Justificación Técnica de Arquitectura: Modelo de Deduplicación e Integridad Fiscal
-- MAGIC 
-- MAGIC **Decisiones de Modelado:**
-- MAGIC 1. **Criterio de Deduplicación:**  
-- MAGIC    - **Llave Seleccionada:** `clave_hacienda`. Es la clave tributaria estándar de 50 dígitos que identifica unívocamente cada factura electrónica a nivel nacional.
-- MAGIC    - **Estrategia Spark SQL:** Se implementó `QUALIFY ROW_NUMBER() OVER (PARTITION BY clave_hacienda ORDER BY fecha_ingesta DESC) = 1`. Garantiza que ante duplicados por reintentos de red o retransmisión de lotes, se mantenga deterministamente el registro con la marca de tiempo de ingesta más reciente.
-- MAGIC 2. **Tratamiento de Nulos y Valores Inválidos:**  
-- MAGIC    - **Política de Filtrado Estricto:** Comprobantes sin `clave_hacienda` o sin monto total son descartados de inmediato, dado que contablemente representan transacciones nulas no deducibles.
-- MAGIC    - **Imputación Defensiva:** Para atributos no críticos se utilizó `coalesce()`: moneda por defecto `CRC`, condición de venta `CONTADO` y departamento asignado a `Operaciones`.

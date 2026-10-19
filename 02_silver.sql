-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 🥈 Pipeline de Facturación B2B · 02 · Limpieza y Enriquecimiento Silver
-- MAGIC 
-- MAGIC **Columna vertebral, parte 2 · Sección 3 del examen (Data Transformation and Cleansing).**
-- MAGIC 
-- MAGIC ### ¿Para qué sirve este paso?
-- MAGIC Los datos crudos en Bronze contienen textos sucios, monedas mixtas (USD y CRC), posibles duplicados de transmisión y requieren enriquecerse.
-- MAGIC Este notebook transforma las compras en **`pf_avalos.silver.facturas_validadas`**:
-- MAGIC 1. **Tipado Estricto:** Convierte textos a timestamps, fechas fiscales (`yyyy-MM`) y números decimales.
-- MAGIC 2. **Normalización Monetaria:** Convierte facturas en dólares a colones (CRC) usando el tipo de cambio oficial de la transacción.
-- MAGIC 3. **Tratamiento Justificado de Nulos:** Regla contable estricta de auditoría.
-- MAGIC 4. **Deduplicación por Llave del Negocio:** La clave fiscal de 50 dígitos (`clave_hacienda`).
-- MAGIC 5. **JOIN Enriquecedor:** Conecta con el catálogo maestro de proveedores (`silver.cat_proveedores`) para obtener actividad, categoría contable y deducibilidad de IVA.
-- MAGIC 6. **Métricas Antes y Después:** Evidencia comparativa requerida por la rúbrica.

-- COMMAND ----------

USE CATALOG pf_avalos;

-- COMMAND ----------

-- Transformación y Limpieza Silver
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
  -- Regla de integridad fiscal: Descartar facturas sin clave tributaria o sin total
  WHERE f.clave_hacienda IS NOT NULL 
    AND f.monto_total IS NOT NULL
    AND f.monto_total >= 0
),
facturas_deduplicadas AS (
  -- Deduplicación por la llave de negocio: conservamos la versión más reciente ingestada
  SELECT *
  FROM facturas_tipadas
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY clave_hacienda 
    ORDER BY fecha_ingesta DESC
  ) = 1
)
-- JOIN con el Catálogo de Proveedores para enriquecer la transacción
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
  -- Conversión a moneda estandarizada (Colones CRC)
  round(CASE WHEN d.moneda = 'USD' THEN d.monto_subtotal_orig * d.tipo_cambio ELSE d.monto_subtotal_orig END, 2) AS subtotal_crc,
  round(CASE WHEN d.moneda = 'USD' THEN d.monto_iva_orig * d.tipo_cambio ELSE d.monto_iva_orig END, 2) AS iva_crc,
  round(CASE WHEN d.moneda = 'USD' THEN d.monto_total_orig * d.tipo_cambio ELSE d.monto_total_orig END, 2) AS total_crc,
  -- Cálculo de Crédito Fiscal: solo se acredita si el proveedor y rubro son deducibles
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
-- MAGIC ### 📊 Métricas Antes y Después (Evidencia Obligatoria)
-- MAGIC Esta consulta compara el volumen y calidad entre la capa cruda (`bronze`) y la capa limpia (`silver`).

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
  'silver (limpia)',
  count(*),
  count(DISTINCT clave_hacienda),
  count(CASE WHEN clave_hacienda IS NULL THEN 1 END),
  sum(total_crc)
FROM silver.facturas_validadas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 📝 RESPUESTA AL EXAMEN DE CERTIFICACIÓN
-- MAGIC 
-- MAGIC **Pregunta del docente:**  
-- MAGIC *¿Qué regla usaste para los nulos y los duplicados, y por qué esa y no otra?*
-- MAGIC 
-- MAGIC **Respuesta técnica (Data Engineer Associate):**  
-- MAGIC 1. **Deduplicación:**  
-- MAGIC    - **Llave elegida:** `clave_hacienda`. Es un estándar tributario de 50 caracteres único a nivel país que incluye código de país, día, mes, año, cédula del emisor y número consecutivo. No puede existir dos veces una misma clave en el sistema tributario.
-- MAGIC    - **Mecanismo:** Se utilizó la cláusula `QUALIFY ROW_NUMBER() OVER (PARTITION BY clave_hacienda ORDER BY fecha_ingesta DESC) = 1`. Se eligió este método porque ante retransmisiones de red o reenvíos de correo, garantiza conservar la versión con la marca de tiempo de ingesta más reciente de forma determinista y eficiente en Spark SQL.
-- MAGIC 2. **Tratamiento de Nulos:**  
-- MAGIC    - **Regla de Rechazo:** Si `clave_hacienda` o `monto_total` son nulos, el registro se descarta en el `WHERE`, ya que contablemente un comprobante sin número fiscal o sin monto es inexistente y violaría la legislación tributaria.
-- MAGIC    - **Regla de Imputación:** Para variables accesorias se usó `coalesce()`: Si `moneda` es nula se asume `CRC` (moneda de curso legal), si `condicion_venta` es nula se asume `CONTADO`, y si `departamento` es nulo se asigna a `Operaciones`.

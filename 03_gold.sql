-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 🥇 Plataforma de Auditoría Fiscal B2B · 03 · Modelo Analítico Gold y Reglas de Calidad
-- MAGIC 
-- MAGIC Este notebook construye la capa **Gold** orientada al consumo analítico de la dirección financiera y contraloría corporativa, aplicando restricciones físicas de integridad (**Delta CHECK Constraints**).
-- MAGIC 
-- MAGIC ### 🎯 Preguntas de Negocio Resueltas:
-- MAGIC 1. **Ejecución Presupuestaria:** ¿Cuál es el gasto mensual consolidado por departamento corporativo y categoría contable?
-- MAGIC 2. **Auditoría Tributaria:** ¿Cuál es el monto exacto de **Crédito Fiscal de IVA** aplicable para la liquidación mensual ante Hacienda?
-- MAGIC 3. **Monitoreo de Compras:** ¿Cuál es el volumen de facturas y el ticket promedio por centro de costos?

-- COMMAND ----------

USE CATALOG pf_avalos;

-- COMMAND ----------

-- Creación de la Tabla Analítica Gold
CREATE OR REPLACE TABLE gold.resumen_gastos_departamento_categoria AS
SELECT
  periodo_fiscal,
  departamento,
  categoria_gasto,
  count(*) AS total_facturas,
  round(sum(total_crc), 2) AS gasto_total_crc,
  round(sum(subtotal_crc), 2) AS gasto_subtotal_crc,
  round(sum(iva_crc), 2) AS total_iva_crc,
  round(sum(iva_credito_fiscal_crc), 2) AS credito_fiscal_iva_deducible_crc,
  round(avg(total_crc), 2) AS ticket_promedio_crc
FROM silver.facturas_validadas
GROUP BY 
  periodo_fiscal,
  departamento,
  categoria_gasto;

SELECT * 
FROM gold.resumen_gastos_departamento_categoria
ORDER BY periodo_fiscal DESC, gasto_total_crc DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🛡️ Restricciones de Calidad en Motor Delta (Constraints)
-- MAGIC Reglas de integridad física para blindar la capa analítica contra transacciones negativas o inconsistencias contables:
-- MAGIC 1. **`gasto_total_positivo`**: El gasto mensual consolidado debe ser estrictamente mayor o igual a cero.
-- MAGIC 2. **`credito_fiscal_valido`**: El crédito fiscal de IVA debe ser mayor o igual a cero.

-- COMMAND ----------

ALTER TABLE gold.resumen_gastos_departamento_categoria DROP CONSTRAINT IF EXISTS gasto_total_positivo;
ALTER TABLE gold.resumen_gastos_departamento_categoria DROP CONSTRAINT IF EXISTS credito_fiscal_valido;

ALTER TABLE gold.resumen_gastos_departamento_categoria 
  ADD CONSTRAINT gasto_total_positivo CHECK (gasto_total_crc >= 0);

ALTER TABLE gold.resumen_gastos_departamento_categoria 
  ADD CONSTRAINT credito_fiscal_valido CHECK (credito_fiscal_iva_deducible_crc >= 0);

SHOW TBLPROPERTIES gold.resumen_gastos_departamento_categoria;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 🚨 Control de Integridad: Validación de Rechazo de Filas Inválidas
-- MAGIC Prueba controlada de inserción con valores monetarios negativos. El motor transaccional Delta debe bloquear atómicamente la escritura protegiendo los reportes financieros.

-- COMMAND ----------

-- INSERCIÓN ANÓMALA CONTROLADA (Prueba de estrés de restricciones de calidad)
-- Comportamiento esperado: La celda debe ser rechazada con excepción 'CHECK constraint gasto_total_positivo violated'
INSERT INTO gold.resumen_gastos_departamento_categoria VALUES
  ('2026-10', 'Tecnologia', 'Auditoria Anomala', 1, -95000.0, -84070.0, -10930.0, -10930.0, -95000.0);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 📋 Justificación Técnica de Arquitectura: Selección de Tabla Delta vs. Vistas
-- MAGIC 
-- MAGIC **Decisiones de Diseño:**
-- MAGIC 1. **Propósito Analítico:** La capa Gold provee métricas financieras consolidadas requeridas para auditorías tributarias y presentación de declaraciones ante el Ministerio de Hacienda.
-- MAGIC 2. **Justificación de Tabla Delta:**
-- MAGIC    - **Vs. Vistas Simples:** Una vista estándar recalcula las agrupaciones en cada lectura, incrementando la latencia y el consumo de cómputo en tableros ejecutivos concurrentes.
-- MAGIC    - **Vs. Materialized Views:** En entornos interactivos serverless dentro de notebooks, las Materialized Views requieren canalizaciones declarativas o SQL Warehouses dedicados.
-- MAGIC    - **Ventajas Delta:** Almacenamiento precomputado físicamente, capacidad de imponer restricciones de integridad (`CHECK constraints`) y soporte nativo para aceleración mediante **Liquid Clustering**.

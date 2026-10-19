-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Proyecto final · 03 · Gold y calidad
-- MAGIC 
-- MAGIC **Componente 1 · Sección 3 del examen.** Una tabla o view gold que responda una pregunta de negocio, y reglas de calidad que se cumplan.
-- MAGIC 
-- MAGIC Lo que se evalúa:
-- MAGIC - Al menos un objeto gold con una pregunta de negocio clara.
-- MAGIC - Al menos **dos** reglas de calidad que Databricks haga cumplir: `NOT NULL`, `CHECK`, o expectations si usas pipelines declarativos.
-- MAGIC - La evidencia de que una fila mala **se rechaza**: el error literal en la salida de una celda.

-- COMMAND ----------

USE CATALOG pf_avalos;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.gastos_por_departamento AS
SELECT 
  departamento,
  categoria_gasto,
  count(*) AS total_facturas,
  round(sum(total_crc), 2) AS gasto_total,
  round(sum(iva_credito_fiscal_crc), 2) AS credito_fiscal_iva
FROM silver.facturas_validadas
GROUP BY departamento, categoria_gasto;

SELECT * FROM gold.gastos_por_departamento;

-- COMMAND ----------

ALTER TABLE gold.gastos_por_departamento DROP CONSTRAINT IF EXISTS gasto_no_negativo;
ALTER TABLE gold.gastos_por_departamento DROP CONSTRAINT IF EXISTS iva_no_negativo;

ALTER TABLE gold.gastos_por_departamento ADD CONSTRAINT gasto_no_negativo CHECK (gasto_total >= 0);
ALTER TABLE gold.gastos_por_departamento ADD CONSTRAINT iva_no_negativo CHECK (credito_fiscal_iva >= 0);

SHOW TBLPROPERTIES gold.gastos_por_departamento;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Evidencia: una fila que se rechaza
-- MAGIC Escribe un `INSERT` con una fila que rompa tu regla y córrelo a mano. **La celda tiene que fallar**: ese error es la evidencia. Déjala fallada en la entrega, y no la incluyas en el job.

-- COMMAND ----------

INSERT INTO gold.gastos_por_departamento VALUES ('Tecnologia', 'Prueba Fallida', 1, -50000.0, -6500.0);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### TU RESPUESTA
-- MAGIC ¿Qué pregunta de negocio responde tu tabla gold? ¿Por qué elegiste tabla, view o materialized view?
-- MAGIC 
-- MAGIC Responde cuánto presupuesto ejecuta la empresa por departamento y categoría, y cuánto crédito fiscal de IVA se puede deducir ante Hacienda. Elegí tabla Delta porque persiste las agregaciones físicamente para reportes rápidos, soporta Liquid Clustering y permite imponer restricciones físicas CHECK que rechazan filas inválidas.

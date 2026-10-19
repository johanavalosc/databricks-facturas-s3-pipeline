-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 🥇 Pipeline de Facturación B2B · 03 · Capa Gold y Calidad de Datos
-- MAGIC 
-- MAGIC **Componente 1 del examen · Sección 3 (Data Modeling, Serving and Data Quality).**
-- MAGIC 
-- MAGIC ### ¿Para qué sirve este paso?
-- MAGIC La capa Gold es la cara visible para la gerencia financiera, contadores y directores ejecutivos.
-- MAGIC Este notebook crea la tabla analítica **`pf_avalos.gold.resumen_gastos_departamento_categoria`** y aplica **restricciones de integridad (Constraints CHECK)** administradas por Databricks para evitar que datos erróneos o fraudulentos ingresen a los reportes.
-- MAGIC 
-- MAGIC ### Preguntas de negocio que responde:
-- MAGIC 1. ¿Cuánto presupuesto ejecuta cada departamento por categoría de gasto cada mes?
-- MAGIC 2. ¿Cuál es el monto exacto de **Crédito Fiscal de IVA** que la empresa puede deducir en la declaración tributaria mensual?
-- MAGIC 3. ¿Cuál es el ticket promedio por compra según el rubro corporativo?

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

-- Visualización de la capa Gold
SELECT * 
FROM gold.resumen_gastos_departamento_categoria
ORDER BY periodo_fiscal DESC, gasto_total_crc DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🛡️ Reglas de Calidad Enforzadas por Databricks (Constraints)
-- MAGIC El examen exige al menos **dos reglas de calidad** que el motor Delta haga cumplir activamente (`NOT NULL` o `CHECK`).
-- MAGIC 1. **`gasto_total_positivo`**: El gasto mensual no puede ser un valor negativo.
-- MAGIC 2. **`credito_fiscal_valido`**: El crédito fiscal de IVA no puede ser negativo.

-- COMMAND ----------

-- Eliminar si existen para permitir re-ejecución limpia
ALTER TABLE gold.resumen_gastos_departamento_categoria DROP CONSTRAINT IF EXISTS gasto_total_positivo;
ALTER TABLE gold.resumen_gastos_departamento_categoria DROP CONSTRAINT IF EXISTS credito_fiscal_valido;

-- Agregar restricciones de calidad activas
ALTER TABLE gold.resumen_gastos_departamento_categoria 
  ADD CONSTRAINT gasto_total_positivo CHECK (gasto_total_crc >= 0);

ALTER TABLE gold.resumen_gastos_departamento_categoria 
  ADD CONSTRAINT credito_fiscal_valido CHECK (credito_fiscal_iva_deducible_crc >= 0);

-- Verificación de las constraints registradas en Delta
SHOW TBLPROPERTIES gold.resumen_gastos_departamento_categoria;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 🚨 Evidencia Obligatoria: Una fila que se rechaza
-- MAGIC **Instrucción de la rúbrica:** Escribe un `INSERT` con una fila que rompa la regla y córrelo a mano.
-- MAGIC **La celda TIENE que fallar:** Ese error en rojo es la evidencia literal de que Databricks protege la tabla. Deja esta celda con el error visible.

-- COMMAND ----------

-- INTENTO DE INSERCIÓN CORRUPTA (MONTO NEGATIVO FRAUDULENTO)
-- Esta celda debe fallar con: CHECK constraint gasto_total_positivo violated
INSERT INTO gold.resumen_gastos_departamento_categoria VALUES
  ('2026-10', 'Tecnologia', 'Auditoria Fallida', 1, -95000.0, -84070.0, -10930.0, -10930.0, -95000.0);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 📝 RESPUESTA AL EXAMEN DE CERTIFICACIÓN
-- MAGIC 
-- MAGIC **Pregunta del docente:**  
-- MAGIC *¿Qué pregunta de negocio responde tu tabla gold? ¿Por qué elegiste tabla, view o materialized view?*
-- MAGIC 
-- MAGIC **Respuesta técnica (Data Engineer Associate):**  
-- MAGIC 1. **Pregunta de Negocio:** Permite al Director Financiero (CFO) y al Contralor auditar el gasto mensual consolidado por centro de costos (departamento) y tipo de proveedor, conociendo de antemano el monto exacto de crédito fiscal de IVA para el formulario D-104 de Hacienda.
-- MAGIC 2. **Por qué Tabla Delta (y no View ni Materialized View):**  
-- MAGIC    - **Vs. View Tradicional:** Una vista ordinaria re-calcula las agregaciones (`SUM`, `AVG`, `GROUP BY`) cada vez que un usuario o dashboard de PowerBI/Tableau la consulta, generando alto costo de cómputo y latencia en consultas repetitivas.
-- MAGIC    - **Vs. Materialized View:** En Databricks Serverless interactivo dentro de notebooks, las Materialized Views no pueden crearse directamente (requieren pipelines declarativos Lakeflow/DLT o SQL Warehouses dedicados).
-- MAGIC    - **Ventajas de la Tabla Delta:** Almacena los resultados físicamente computados, soporta **Liquid Clustering** para acelerar reportes ejecutivos, permite habilitar **Constraints CHECK** (las vistas no imponen restricciones físicas de escritura) y ofrece aislamiento ACID garantizado con historial de versiones (*Time Travel*).

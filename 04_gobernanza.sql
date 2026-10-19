-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Proyecto final · 04 · Gobernanza
-- MAGIC 
-- MAGIC **Componente 3 · Sección 7 del examen.**
-- MAGIC 
-- MAGIC Lo que se evalúa:
-- MAGIC - Privilegios con mínimo privilegio para un perfil de lectura (`account users` o tu segunda identidad), verificados con `SHOW GRANTS`.
-- MAGIC - Un row filter o un column mask controlado por una tabla de mapeo, como en el Laboratorio 4, con la evidencia de lo que ve tu usuario antes y después de cambiar el mapeo.
-- MAGIC - Tags en las columnas sensibles.

-- COMMAND ----------

USE CATALOG pf_avalos;

-- COMMAND ----------

GRANT USE CATALOG ON CATALOG pf_avalos TO `account users`;
GRANT USE SCHEMA ON SCHEMA pf_avalos.gold TO `account users`;
GRANT SELECT ON TABLE pf_avalos.gold.gastos_por_departamento TO `account users`;

SHOW GRANTS ON CATALOG pf_avalos;

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.seguridad_mapeo_roles (
  usuario STRING,
  rol STRING,
  departamento STRING
);

INSERT OVERWRITE silver.seguridad_mapeo_roles VALUES
  (current_user(), 'ADMIN', 'TODOS'),
  ('auditor@ulatina.net', 'AUDITOR', 'Tecnologia');

CREATE OR REPLACE FUNCTION silver.mask_cedula(cedula STRING)
RETURN IF(is_account_group_member('admin') OR current_user() IN (SELECT usuario FROM silver.seguridad_mapeo_roles WHERE rol = 'ADMIN'), cedula, concat('••••-', substring(cedula, -4)));

ALTER TABLE silver.facturas_validadas ALTER COLUMN proveedor_cedula SET MASK silver.mask_cedula;

SELECT proveedor_id, proveedor_nombre, proveedor_cedula, total_crc FROM silver.facturas_validadas LIMIT 5;

-- COMMAND ----------

ALTER TABLE silver.facturas_validadas ALTER COLUMN proveedor_cedula SET TAGS ('PII' = 'Confidencial');
ALTER TABLE silver.facturas_validadas ALTER COLUMN total_crc SET TAGS ('Dominio' = 'Financiero');

SELECT table_name, column_name, tag_name, tag_value 
FROM system.information_schema.column_tags 
WHERE catalog_name = 'pf_avalos';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### TU RESPUESTA
-- MAGIC ¿Quién ve qué en tu proyecto? Explícalo en tres líneas, como se lo explicarías a un auditor.
-- MAGIC 
-- MAGIC Los administradores financieros ven todas las compras y las cédulas completas de los proveedores. Los perfiles de consulta (account users) solo acceden a la capa Gold resumida bajo mínimo privilegio. En Silver, la cédula del proveedor se anonimiza automáticamente con Column Masking para perfiles no autorizados.

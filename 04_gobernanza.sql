-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 🛡️ Plataforma de Auditoría Fiscal B2B · 04 · Gobernanza en Unity Catalog
-- MAGIC 
-- MAGIC Este notebook implementa la capa de seguridad, control de accesos basados en atributos (ABAC) y protección de datos confidenciales (**PII**) en **Unity Catalog**.
-- MAGIC 
-- MAGIC ### 🎯 Arquitectura de Seguridad y Cumplimiento:
-- MAGIC 1. **Mínimo Privilegio (RBAC):** Otorgamiento controlado de permisos de solo lectura (`USE CATALOG`, `USE SCHEMA`, `SELECT`).
-- MAGIC 2. **Anonimización Dinámica (Column Masking):** Enmascaramiento de cédulas jurídicas y datos fiscales para perfiles no financieros (`••••-••••-XXXX`).
-- MAGIC 3. **Segregación Departamental (Row Filtering):** Aislamiento granular de compras para que cada departamento solo audite sus propios gastos.
-- MAGIC 4. **Clasificación de Datos (Tags):** Etiquetado de columnas sensibles para auditorías normativas.

-- COMMAND ----------

USE CATALOG pf_avalos;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Concesión de Permisos de Mínimo Privilegio (SHOW GRANTS)
-- MAGIC Acceso de consulta estrictamente delimitado a la capa Gold para usuarios generales de la cuenta.

-- COMMAND ----------

GRANT USE CATALOG ON CATALOG pf_avalos TO `account users`;
GRANT USE SCHEMA ON SCHEMA pf_avalos.gold TO `account users`;
GRANT SELECT ON TABLE pf_avalos.gold.resumen_gastos_departamento_categoria TO `account users`;

SHOW GRANTS ON CATALOG pf_avalos;

-- COMMAND ----------

SHOW GRANTS ON TABLE pf_avalos.gold.resumen_gastos_departamento_categoria;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Seguridad Dinámica con Tabla de Mapeo de Roles

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.seguridad_mapeo_roles (
  usuario STRING NOT NULL,
  rol STRING NOT NULL,
  departamento_autorizado STRING NOT NULL
);

INSERT OVERWRITE silver.seguridad_mapeo_roles VALUES
  (current_user(), 'ADMIN_FINANCIERO', 'TODOS'),
  ('auditor_externo@empresa.com', 'AUDITOR', 'Tecnologia'),
  ('analista_compras@empresa.com', 'OPERADOR', 'Operaciones');

SELECT * FROM silver.seguridad_mapeo_roles;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 🎭 Column Masking: Anonimización de Identificación Fiscal
-- MAGIC Usuarios con rol `ADMIN_FINANCIERO` visualizan el identificador completo. Perfiles regulares reciben la cadena con los dígitos centrales ofuscados.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION silver.enmascarar_cedula(cedula STRING)
RETURNS STRING
RETURN IF(
  is_account_group_member('admin') OR current_user() IN (
    SELECT usuario FROM silver.seguridad_mapeo_roles WHERE rol = 'ADMIN_FINANCIERO'
  ),
  cedula,
  concat('••••-••••-', substring(cedula, -4))
);

ALTER TABLE silver.facturas_validadas 
  ALTER COLUMN proveedor_cedula SET MASK silver.enmascarar_cedula;

SELECT proveedor_id, proveedor_nombre, proveedor_cedula, total_crc
FROM silver.facturas_validadas
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 🔍 Row Filter: Filtrado Granular por Centro de Costos
-- MAGIC Cada usuario visualiza únicamente los registros correspondientes a su departamento de adscripción.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION gold.filtro_departamento(depto STRING)
RETURNS BOOLEAN
RETURN IF(
  is_account_group_member('admin') OR EXISTS (
    SELECT 1 FROM silver.seguridad_mapeo_roles 
    WHERE usuario = current_user() AND (rol = 'ADMIN_FINANCIERO' OR departamento_autorizado = depto OR departamento_autorizado = 'TODOS')
  ),
  true,
  false
);

ALTER TABLE gold.resumen_gastos_departamento_categoria 
  SET ROW FILTER gold.filtro_departamento ON (departamento);

SELECT * FROM gold.resumen_gastos_departamento_categoria;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Clasificación de Metadatos y Tags de Cumplimiento

-- COMMAND ----------

ALTER TABLE silver.facturas_validadas 
  ALTER COLUMN proveedor_cedula SET TAGS ('Clasificacion' = 'PII', 'Sensibilidad' = 'Confidencial');

ALTER TABLE silver.facturas_validadas 
  ALTER COLUMN total_crc SET TAGS ('Dominio' = 'Financiero', 'Cumplimiento' = 'Hacienda');

SELECT 
  table_name,
  column_name,
  tag_name,
  tag_value
FROM system.information_schema.column_tags
WHERE catalog_name = 'pf_avalos'
ORDER BY table_name, column_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 📋 Matriz de Control de Accesos y Política de Seguridad
-- MAGIC 
-- MAGIC **Resumen de Gobernanza para Auditoría:**
-- MAGIC 1. **Dirección Financiera (`ADMIN_FINANCIERO`):** Acceso irrestricto transversal a todos los centros de costos y visualización desanonimizada de cédulas de proveedores para liquidación de pagos.
-- MAGIC 2. **Líderes de Departamento:** Acceso acotado exclusivamente a las transacciones de su centro de costos autorizado mediante **Row Filtering** dinámico.
-- MAGIC 3. **Perfiles de Consulta Externa (`account users`):** Concesión de mínimo privilegio sobre la capa Gold, con ofuscación obligatoria de identificadores mediante **Column Masking** en Silver.

-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 🛡️ Pipeline de Facturación B2B · 04 · Gobernanza en Unity Catalog
-- MAGIC 
-- MAGIC **Componente 3 del examen · Sección 7 (Data Governance, Security, and Compliance).**
-- MAGIC 
-- MAGIC ### ¿Para qué sirve este paso?
-- MAGIC En el sector empresarial y financiero, las compras contienen **datos confidenciales (PII)** como números de identificación fiscal (cédulas jurídicas), cuentas bancarias y márgenes comerciales.
-- MAGIC Este notebook implementa un modelo de seguridad basado en roles (RBAC) y atributos (ABAC):
-- MAGIC 1. **Mínimo Privilegio:** Acceso estricto concedido a perfiles de solo lectura (`USE CATALOG`, `USE SCHEMA`, `SELECT`).
-- MAGIC 2. **Seguridad Dinámica con Tablas de Mapeo:**
-- MAGIC    - **Column Masking:** Anonimiza la cédula jurídica del proveedor (`••••-••••-1234`) si quien consulta no pertenece al equipo financiero.
-- MAGIC    - **Row Filter:** Restringe las filas para que un gerente solo audite las compras de su propio departamento.
-- MAGIC 3. **Tags de Metadatos Sensibles:** Clasificación de datos para cumplimiento normativo (`PII`, `Hacienda`).

-- COMMAND ----------

USE CATALOG pf_avalos;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Permisos de Mínimo Privilegio (SHOW GRANTS)
-- MAGIC Para que un perfil lector pueda ver datos en Unity Catalog necesita estrictamente:
-- MAGIC - `USE CATALOG` en el catálogo.
-- MAGIC - `USE SCHEMA` en el esquema destino (`gold`).
-- MAGIC - `SELECT` únicamente sobre la tabla analítica final.

-- COMMAND ----------

-- Conceder accesos de solo lectura a usuarios de la cuenta
GRANT USE CATALOG ON CATALOG pf_avalos TO `account users`;
GRANT USE SCHEMA ON SCHEMA pf_avalos.gold TO `account users`;
GRANT SELECT ON TABLE pf_avalos.gold.resumen_gastos_departamento_categoria TO `account users`;

-- Verificación de privilegios otorgados
SHOW GRANTS ON CATALOG pf_avalos;

-- COMMAND ----------

SHOW GRANTS ON TABLE pf_avalos.gold.resumen_gastos_departamento_categoria;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Tabla de Mapeo de Seguridad y Funciones de Enmascaramiento

-- COMMAND ----------

-- Tabla de Mapeo que define qué puede ver cada usuario
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
-- MAGIC ### 🎭 Column Masking: Anonimización de Cédulas Jurídicas
-- MAGIC Si el usuario logueado (`current_user()`) es `ADMIN_FINANCIERO`, ve la cédula real. De lo contrario, se enmascaran los primeros dígitos dejando solo los últimos 4.

-- COMMAND ----------

-- Crear función de enmascaramiento
CREATE OR REPLACE FUNCTION silver.enmascarar_cedula(cedula STRING)
RETURNS STRING
RETURN IF(
  is_account_group_member('admin') OR current_user() IN (
    SELECT usuario FROM silver.seguridad_mapeo_roles WHERE rol = 'ADMIN_FINANCIERO'
  ),
  cedula,
  concat('••••-••••-', substring(cedula, -4))
);

-- Aplicar la máscara a la columna sensible en Silver
ALTER TABLE silver.facturas_validadas 
  ALTER COLUMN proveedor_cedula SET MASK silver.enmascarar_cedula;

-- Prueba de visualización protegida
SELECT proveedor_id, proveedor_nombre, proveedor_cedula, total_crc
FROM silver.facturas_validadas
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 🔍 Row Filter: Filtrado por Departamento Autorizado
-- MAGIC Un auditor o gerente solo ve los gastos de su centro de costos autorizado.

-- COMMAND ----------

-- Crear función de filtro de filas
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

-- Aplicar Row Filter sobre la tabla Gold
ALTER TABLE gold.resumen_gastos_departamento_categoria 
  SET ROW FILTER gold.filtro_departamento ON (departamento);

-- Verificación de filas visibles bajo tu identidad actual
SELECT * FROM gold.resumen_gastos_departamento_categoria;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Tags en Columnas Sensibles (Clasificación de Datos)
-- MAGIC Etiquetado de metadatos gobernados en Unity Catalog para auditorías de cumplimiento (GDPR, Ciberseguridad y Tributario).

-- COMMAND ----------

-- Aplicar etiquetas a columnas críticas
ALTER TABLE silver.facturas_validadas 
  ALTER COLUMN proveedor_cedula SET TAGS ('Clasificacion' = 'PII', 'Sensibilidad' = 'Confidencial');

ALTER TABLE silver.facturas_validadas 
  ALTER COLUMN total_crc SET TAGS ('Dominio' = 'Financiero', 'Cumplimiento' = 'Hacienda');

-- Consultar catálogo de tags del sistema
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
-- MAGIC ### 📝 RESPUESTA AL EXAMEN DE CERTIFICACIÓN
-- MAGIC 
-- MAGIC **Pregunta del docente:**  
-- MAGIC *¿Quién ve qué en tu proyecto? Explícalo en tres líneas, como se lo explicarías a un auditor.*
-- MAGIC 
-- MAGIC **Respuesta técnica (Data Engineer Associate):**  
-- MAGIC 1. **Administración y Finanzas (`ADMIN_FINANCIERO`):** Tienen acceso irrestricto a todos los departamentos y visualizan la cédula jurídica íntegra de los proveedores para liquidación de pagos y conciliación bancaria.
-- MAGIC 2. **Líderes de Departamento y Operadores:** Mediante el **Row Filter**, están restringidos exclusivamente a auditar los gastos de su propio centro de costos (p. ej., Tecnología solo ve facturas de TI).
-- MAGIC 3. **Auditores Generales y Perfiles Lectores (`account users`):** Solo acceden a la capa Gold resumida bajo mínimo privilegio, mientras que en Silver el **Column Masking** anonimiza automáticamente los números de identificación fiscal a formato `••••-••••-XXXX`.

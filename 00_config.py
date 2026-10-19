# Databricks notebook source
# MAGIC %md
# MAGIC # 🏢 Pipeline de Facturación B2B · 00 · Configuración Inicial y Sincronización S3
# MAGIC 
# MAGIC Este notebook establece la base de gobernanza en **Unity Catalog** para el proyecto de auditoría fiscal y control de compras empresariales.
# MAGIC 
# MAGIC ### ¿Para qué sirve este paso?
# MAGIC 1. Crea el catálogo gobernado **`pf_avalos`** y los 3 esquemas de la **Arquitectura Medallion** (`bronze`, `silver`, `gold`).
# MAGIC 2. Crea el **Volume administrado** `pf_avalos.bronze.landing` donde aterrizan los archivos crudos.
# MAGIC 3. Provee la tabla dimensional maestra de **Proveedores** (`silver.cat_proveedores`) para enriquecer las compras mediante un `JOIN`.
# MAGIC 4. Sincroniza las facturas electrónicas desde el bucket **AWS S3** (`nautilus-facturas01`) hacia el Volume organizadas por **lotes** (`lote_1`, `lote_2`), permitiendo evaluar la ingesta incremental de Auto Loader.
# MAGIC 
# MAGIC ### ¿Por qué lo pide el profesor para el examen?
# MAGIC - **Sección 7 del examen (Data Governance):** Evalúa la jerarquía de 3 niveles de Unity Catalog (`catalog.schema.table_or_volume`) y el uso de Volumes administrados en lugar del DBFS tradicional en desuso.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Creación de la jerarquía Medallion en Unity Catalog
# MAGIC CREATE CATALOG IF NOT EXISTS pf_avalos
# MAGIC   COMMENT 'Catálogo central de auditoría y análisis de gastos empresariales';
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS pf_avalos.bronze
# MAGIC   COMMENT 'Capa Bronze: Archivos raw, metadatos de archivo de origen e ingesta streaming';
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS pf_avalos.silver
# MAGIC   COMMENT 'Capa Silver: Datos limpios, normalizados a CRC, deduplicados y enriquecidos';
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS pf_avalos.gold
# MAGIC   COMMENT 'Capa Gold: Resumen analítico de gastos, crédito fiscal IVA y reglas CHECK';
# MAGIC 
# MAGIC -- Volume para el aterrizaje de archivos desde S3
# MAGIC CREATE VOLUME IF NOT EXISTS pf_avalos.bronze.landing
# MAGIC   COMMENT 'Volume administrado para el almacenamiento temporal de facturas JSON/XML';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Catálogo Maestro de Proveedores (Tabla Dimensional para el JOIN)
# MAGIC En el examen y en la vida real, una tabla de transacciones de compra debe enriquecerse con información del proveedor (actividad económica, régimen de deducibilidad, categoría corporativa).

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG pf_avalos;
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE silver.cat_proveedores (
# MAGIC   proveedor_id STRING NOT NULL,
# MAGIC   cedula_juridica STRING NOT NULL,
# MAGIC   razon_social STRING NOT NULL,
# MAGIC   categoria_gasto STRING NOT NULL,
# MAGIC   es_deducible_iva BOOLEAN NOT NULL,
# MAGIC   plazo_pago_dias INT
# MAGIC )
# MAGIC COMMENT 'Catálogo maestro de proveedores homologados para compras empresariales';
# MAGIC 
# MAGIC INSERT OVERWRITE silver.cat_proveedores VALUES
# MAGIC   ('PRV-001', '3-101-554433', 'DISTRIBUIDORA Y REPUESTOS LA CENTRAL S.A.', 'Repuestos y Mantenimiento', true, 30),
# MAGIC   ('PRV-002', '3-101-223344', 'ESTACION DE SERVICIO Y COMBUSTIBLES DELTA S.A.', 'Combustibles y Lubricantes', true, 0),
# MAGIC   ('PRV-003', '3-101-998877', 'SERVICIOS CLOUD AMAZON WEB SERVICES CR S.A.', 'Tecnologia y Nube', true, 15),
# MAGIC   ('PRV-004', '3-102-445566', 'SUMINISTROS Y PAPELERIA OFFICE DEPOT CR S.A.', 'Materiales y Suministros', true, 30),
# MAGIC   ('PRV-005', '3-101-112233', 'INSTITUTO COSTARRICENSE DE ELECTRICIDAD (ICE)', 'Servicios Publicos y Telecom', true, 15),
# MAGIC   ('PRV-006', '3-101-778899', 'COOPERATIVA DE PRODUCTORES DE LECHE DOS PINOS R.L.', 'Alimentos y Comedor', true, 7);
# MAGIC 
# MAGIC SELECT * FROM silver.cat_proveedores;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Sincronización y Generación de Lotes (Tema O7: Ingesta desde S3)
# MAGIC Selecciona el lote para probar la ingesta incremental de Auto Loader:
# MAGIC - **Lote 1:** Primer conjunto de facturas (5 compras corporativas).
# MAGIC - **Lote 2:** Segundo conjunto de facturas (nuevas compras que no deben duplicar el lote 1).

# COMMAND ----------

import json
import os

# Widget interactivo para seleccionar el lote
dbutils.widgets.dropdown("lote", "1", ["1", "2"], "Lote de Facturas")
lote_seleccionado = dbutils.widgets.get("lote")

ruta_destino = f"/Volumes/pf_avalos/bronze/landing/facturas/lote_{lote_seleccionado}"

# Datos de facturas electrónicas reales estructuradas en JSON
facturas_lote_1 = [
    {
        "clave_hacienda": "50619102600310155443300100001010000000001198765432",
        "consecutivo": "00100001010000000001",
        "fecha_emision": "2026-10-15T08:30:00Z",
        "proveedor_id": "PRV-001",
        "departamento": "Operaciones",
        "concepto": "Aceite Sintetico Diesel 15W40 y Filtros Cummins",
        "moneda": "CRC",
        "tipo_cambio": 1.0,
        "monto_subtotal": 277000.0,
        "monto_iva": 36010.0,
        "monto_total": 313010.0,
        "condicion_venta": "CREDITO"
    },
    {
        "clave_hacienda": "50619102600310122334400100001010000000002145678901",
        "consecutivo": "00100001010000000002",
        "fecha_emision": "2026-10-15T09:15:00Z",
        "proveedor_id": "PRV-002",
        "departamento": "Transporte",
        "concepto": "Combustible Diesel Flotilla y Aditivos",
        "moneda": "CRC",
        "tipo_cambio": 1.0,
        "monto_subtotal": 245100.0,
        "monto_iva": 2314.0,
        "monto_total": 247414.0,
        "condicion_venta": "CONTADO"
    },
    {
        "clave_hacienda": "50619102600310199887700100001010000000003189012345",
        "consecutivo": "00100001010000000003",
        "fecha_emision": "2026-10-16T10:00:00Z",
        "proveedor_id": "PRV-003",
        "departamento": "Tecnologia",
        "concepto": "Suscripcion Mensual AWS Compute & S3 Storage",
        "moneda": "USD",
        "tipo_cambio": 518.5,
        "monto_subtotal": 1250.0,
        "monto_iva": 162.5,
        "monto_total": 1412.5,
        "condicion_venta": "CREDITO"
    },
    {
        "clave_hacienda": "50619102600310244556600100001010000000004123456789",
        "consecutivo": "00100001010000000004",
        "fecha_emision": "2026-10-16T14:20:00Z",
        "proveedor_id": "PRV-004",
        "departamento": "Administracion",
        "concepto": "Papeleria, toner de impresion y suministros de oficina",
        "moneda": "CRC",
        "tipo_cambio": 1.0,
        "monto_subtotal": 85000.0,
        "monto_iva": 11050.0,
        "monto_total": 96050.0,
        "condicion_venta": "CONTADO"
    },
    {
        "clave_hacienda": "50619102600310111223300100001010000000005167890123",
        "consecutivo": "00100001010000000005",
        "fecha_emision": "2026-10-17T11:05:00Z",
        "proveedor_id": "PRV-005",
        "departamento": "Tecnologia",
        "concepto": "Enlace de Fibra Optica Dedicada 500Mbps Casa Matriz",
        "moneda": "CRC",
        "tipo_cambio": 1.0,
        "monto_subtotal": 145000.0,
        "monto_iva": 18850.0,
        "monto_total": 163850.0,
        "condicion_venta": "CREDITO"
    }
]

facturas_lote_2 = [
    {
        "clave_hacienda": "50619102600310177889900100001010000000006134567890",
        "consecutivo": "00100001010000000006",
        "fecha_emision": "2026-10-18T08:00:00Z",
        "proveedor_id": "PRV-006",
        "departamento": "Administracion",
        "concepto": "Insumos lacteos y cafeteria para comedor de empleados",
        "moneda": "CRC",
        "tipo_cambio": 1.0,
        "monto_subtotal": 62000.0,
        "monto_iva": 8060.0,
        "monto_total": 70060.0,
        "condicion_venta": "CONTADO"
    },
    {
        "clave_hacienda": "50619102600310199887700100001010000000007190123456",
        "consecutivo": "00100001010000000007",
        "fecha_emision": "2026-10-18T16:45:00Z",
        "proveedor_id": "PRV-003",
        "departamento": "Tecnologia",
        "concepto": "Servicios de Seguridad AWS WAF y Shield",
        "moneda": "USD",
        "tipo_cambio": 519.0,
        "monto_subtotal": 450.0,
        "monto_iva": 58.5,
        "monto_total": 508.5,
        "condicion_venta": "CREDITO"
    },
    {
        "clave_hacienda": "50619102600310155443300100001010000000008123450987",
        "consecutivo": "00100001010000000008",
        "fecha_emision": "2026-10-19T09:30:00Z",
        "proveedor_id": "PRV-001",
        "departamento": "Operaciones",
        "concepto": "Baterias 12V Heavy Duty para Camiones de Distribucion",
        "moneda": "CRC",
        "tipo_cambio": 1.0,
        "monto_subtotal": 195000.0,
        "monto_iva": 25350.0,
        "monto_total": 220350.0,
        "condicion_venta": "CREDITO"
    }
]

# Seleccionar datos según el lote
datos_actuales = facturas_lote_1 if lote_seleccionado == "1" else facturas_lote_2

# Escribir en el Volume en formato JSON
df_lote = spark.createDataFrame(datos_actuales)
(df_lote.write
   .mode("overwrite")
   .json(ruta_destino))

print(f"✅ Lote {lote_seleccionado} escrito exitosamente en: {ruta_destino}")
display(dbutils.fs.ls("/Volumes/pf_avalos/bronze/landing/facturas/"))

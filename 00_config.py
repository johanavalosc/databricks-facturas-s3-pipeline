# Databricks notebook source
# MAGIC %md
# MAGIC # Proyecto final · 00 · Configuración
# MAGIC
# MAGIC Este notebook crea el catálogo `pf_avalos`, los esquemas por capa y el volume de aterrizaje.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS pf_avalos;
# MAGIC CREATE SCHEMA IF NOT EXISTS pf_avalos.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS pf_avalos.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS pf_avalos.gold;
# MAGIC CREATE VOLUME IF NOT EXISTS pf_avalos.bronze.landing;

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
# MAGIC );
# MAGIC 
# MAGIC INSERT OVERWRITE silver.cat_proveedores VALUES
# MAGIC   ('PRV-001', '3-101-554433', 'DISTRIBUIDORA LA CENTRAL S.A.', 'Repuestos', true, 30),
# MAGIC   ('PRV-002', '3-101-223344', 'ESTACION SERVICIO DELTA S.A.', 'Combustibles', true, 0),
# MAGIC   ('PRV-003', '3-101-998877', 'AMAZON WEB SERVICES CR S.A.', 'Cloud', true, 15),
# MAGIC   ('PRV-004', '3-102-445566', 'OFFICE DEPOT CR S.A.', 'Suministros', true, 30),
# MAGIC   ('PRV-005', '3-101-112233', 'ICE TELECOMUNICACIONES', 'Servicios', true, 15),
# MAGIC   ('PRV-006', '3-101-778899', 'DOS PINOS R.L.', 'Alimentos', true, 7);
# MAGIC 
# MAGIC SELECT * FROM silver.cat_proveedores;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Datos en lotes
# MAGIC Escribe las facturas en `landing/facturas/lote_N` para validar la ingesta incremental.

# COMMAND ----------

import json
import os

dbutils.widgets.dropdown("lote", "1", ["1", "2"], "Lote")
lote = dbutils.widgets.get("lote")
destino = f"/Volumes/pf_avalos/bronze/landing/facturas/lote_{lote}"

facturas_lote_1 = [
    {"clave_hacienda": "50619102600310155443300100001010000000001198765432", "consecutivo": "00100001010000000001", "fecha_emision": "2026-10-15T08:30:00Z", "proveedor_id": "PRV-001", "departamento": "Operaciones", "concepto": "Aceite Diesel y Filtros", "moneda": "CRC", "tipo_cambio": 1.0, "monto_subtotal": 277000.0, "monto_iva": 36010.0, "monto_total": 313010.0, "condicion_venta": "CREDITO"},
    {"clave_hacienda": "50619102600310122334400100001010000000002145678901", "consecutivo": "00100001010000000002", "fecha_emision": "2026-10-15T09:15:00Z", "proveedor_id": "PRV-002", "departamento": "Transporte", "concepto": "Combustible Diesel Flotilla", "moneda": "CRC", "tipo_cambio": 1.0, "monto_subtotal": 245100.0, "monto_iva": 2314.0, "monto_total": 247414.0, "condicion_venta": "CONTADO"},
    {"clave_hacienda": "50619102600310199887700100001010000000003189012345", "consecutivo": "00100001010000000003", "fecha_emision": "2026-10-16T10:00:00Z", "proveedor_id": "PRV-003", "departamento": "Tecnologia", "concepto": "Suscripcion AWS Cloud", "moneda": "USD", "tipo_cambio": 518.5, "monto_subtotal": 1250.0, "monto_iva": 162.5, "monto_total": 1412.5, "condicion_venta": "CREDITO"},
    {"clave_hacienda": "50619102600310244556600100001010000000004123456789", "consecutivo": "00100001010000000004", "fecha_emision": "2026-10-16T14:20:00Z", "proveedor_id": "PRV-004", "departamento": "Administracion", "concepto": "Papeleria y suministros", "moneda": "CRC", "tipo_cambio": 1.0, "monto_subtotal": 85000.0, "monto_iva": 11050.0, "monto_total": 96050.0, "condicion_venta": "CONTADO"},
    {"clave_hacienda": "50619102600310111223300100001010000000005167890123", "consecutivo": "00100001010000000005", "fecha_emision": "2026-10-17T11:05:00Z", "proveedor_id": "PRV-005", "departamento": "Tecnologia", "concepto": "Internet Fibra Optica", "moneda": "CRC", "tipo_cambio": 1.0, "monto_subtotal": 145000.0, "monto_iva": 18850.0, "monto_total": 163850.0, "condicion_venta": "CREDITO"}
]

facturas_lote_2 = [
    {"clave_hacienda": "50619102600310177889900100001010000000006134567890", "consecutivo": "00100001010000000006", "fecha_emision": "2026-10-18T08:00:00Z", "proveedor_id": "PRV-006", "departamento": "Administracion", "concepto": "Insumos comedor", "moneda": "CRC", "tipo_cambio": 1.0, "monto_subtotal": 62000.0, "monto_iva": 8060.0, "monto_total": 70060.0, "condicion_venta": "CONTADO"},
    {"clave_hacienda": "50619102600310199887700100001010000000007190123456", "consecutivo": "00100001010000000007", "fecha_emision": "2026-10-18T16:45:00Z", "proveedor_id": "PRV-003", "departamento": "Tecnologia", "concepto": "Seguridad AWS WAF", "moneda": "USD", "tipo_cambio": 519.0, "monto_subtotal": 450.0, "monto_iva": 58.5, "monto_total": 508.5, "condicion_venta": "CREDITO"},
    {"clave_hacienda": "50619102600310155443300100001010000000008123450987", "consecutivo": "00100001010000000008", "fecha_emision": "2026-10-19T09:30:00Z", "proveedor_id": "PRV-001", "departamento": "Operaciones", "concepto": "Baterias Camiones", "moneda": "CRC", "tipo_cambio": 1.0, "monto_subtotal": 195000.0, "monto_iva": 25350.0, "monto_total": 220350.0, "condicion_venta": "CREDITO"}
]

datos = facturas_lote_1 if lote == "1" else facturas_lote_2
spark.createDataFrame(datos).write.mode("overwrite").json(destino)

display(dbutils.fs.ls("/Volumes/pf_avalos/bronze/landing/facturas/"))

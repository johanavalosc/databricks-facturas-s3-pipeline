# 🚀 Enterprise B2B Expense & Fiscal Audit Lakehouse
> **Arquitectura Medallion en Databricks con Unity Catalog, Auto Loader, Liquid Clustering y Orquestación Lakeflow Jobs.**  
> **Estudiante:** Johan Ávalos Campos  
> **Programa:** Técnico en Ingeniería de Datos con Databricks · Universidad Latina de Costa Rica  
> **Módulo 4:** Gestión y Gobernanza de Datos con Unity Catalog  
> **Docente:** Andrés Calvo Vargas  

---

## 📌 1. Problema de Negocio y Origen de los Datos

### El Problema
En las organizaciones corporativas, la recepción de facturas comerciales y comprobantes de compra es un proceso descentralizado, caótico y propenso a fraudes o inconsistencias tributarias. Las facturas llegan en múltiples formatos y monedas (USD y CRC), con riesgo de pagos duplicados, categorizaciones de gasto erróneas y pérdida de **créditos fiscales de IVA** deducibles ante la autoridad tributaria (Ministerio de Hacienda de Costa Rica).

### La Solución
Construir una plataforma centralizada sobre el **Lakehouse de Databricks** que:
1. Reciba de forma automatizada las facturas emitidas por proveedores corporativos desde un bucket cloud en **AWS S3**.
2. Procese los archivos de manera incremental mediante **Auto Loader (`cloudFiles`)** sin duplicar registros.
3. Estandarice monedas, sanee nulos, valide tipos de cambio y enriquezca las transacciones mediante cruces relacionales con un catálogo dimensional homologado.
4. Aplique **reglas de calidad de datos (`CHECK constraints`)** que rechacen automáticamente comprobantes con anomalías o montos negativos.
5. Gobierne el acceso a información sensible mediante **Unity Catalog** utilizando **Column Masking** para números de identificación fiscal (cédulas jurídicas) y **Row Filtering** por centro de costos departamental.
6. Acelere el consumo analítico para dashboards ejecutivos mediante **Liquid Clustering** y telemetría de auditoría en **System Tables**.

### Origen de los Datos
- **Fuente Primaria:** Facturas electrónicas estructuradas basadas en el estándar de comprobantes electrónicos v4.4 (Hacienda Costa Rica), sincronizadas desde un bucket privado de **AWS S3 (`nautilus-facturas01`)** hacia el Volume administrado de Unity Catalog (`pf_avalos.bronze.landing`).
- **Lotes de Evaluación:** Dos lotes de compras corporativas (`lote_1` y `lote_2`) con facturas de proveedores reales (Amazon Web Services CR, Estación de Servicio Delta, Repuestos La Central, Office Depot, ICE, Dos Pinos).

---

## 🏗️ 2. Diagrama de Arquitectura End-to-End

```mermaid
graph TD
    subgraph ☁️ 1. Ingesta Cloud & Storage
        A[Facturas Electrónicas XML/JSON] --> B[AWS S3 Bucket: nautilus-facturas01]
        B --> C[(Volume Unity Catalog: pf_avalos.bronze.landing)]
    end

    subgraph 🧱 2. Databricks Medallion Architecture
        C -->|01_bronze.py: Auto Loader cloudFiles| D[(🥉 Bronze: pf_avalos.bronze.facturas_raw<br/>+ _metadata.file_path + fecha_ingesta)]
        D -->|02_silver.sql: Tipado, Deduplicación, Normalización CRC| E[(🥈 Silver: pf_avalos.silver.facturas_validadas)]
        F[(Catálogo Proveedores: silver.cat_proveedores)] -->|INNER JOIN| E
        E -->|03_gold.sql: Agregación Analítica & CHECK Constraints| G[(🥇 Gold: pf_avalos.gold.resumen_gastos_departamento_categoria)]
    end

    subgraph 🛡️ 3. Unity Catalog Security & Governance
        H[Tabla Mapeo: silver.seguridad_mapeo_roles] -.->|Column Masking| E
        H -.->|Row Filter por Departamento| G
        I[Tags PII y Hacienda] -.-> E
        J[Least-Privilege: account users] -.-> G
    end

    subgraph ⚡ 4. Observabilidad, Optimización & Orquestación
        K[Liquid Clustering: CLUSTER BY periodo_fiscal, categoria_gasto] --> E
        L[Compactación Física: OPTIMIZE] --> E
        M[Consultas de Auditoría: system.access.audit & job_run_timeline]
        N[Lakeflow Job DAG: 3 Tareas + Reintentos]
        O[CI/CD: GitHub + databricks.yml DABs]
    end
```

---

## 📋 3. Componentes Evaluados y Temas Opcionales

| Código | Componente del Examen | Implementación en este Proyecto | Estado |
| :---: | :--- | :--- | :---: |
| **V1** | **Bronze (Columna Vertebral)** | Ingesta incremental con Auto Loader (`format("cloudFiles")`), `schemaLocation`, metadatos `_metadata.file_path` y evidencia de lotes. | ✅ Completo |
| **V2** | **Silver (Columna Vertebral)** | Casting estricto, normalización cambiaria USD $\to$ CRC, deduplicación por clave fiscal de 50 dígitos, `INNER JOIN` con catálogo de proveedores y métricas comparativas antes/después. | ✅ Completo |
| **C1** | **Gold y Calidad** | Tabla analítica de gasto y crédito fiscal de IVA, 2 constraints `CHECK` (`gasto_total_positivo`, `credito_fiscal_valido`) y celda con error literal de rechazo. | ✅ Completo |
| **C2** | **Orquestación Lakeflow** | Lakeflow Job con DAG de 3 tareas dependientes (`01_bronze` $\to$ `02_silver` $\to$ `03_gold`), reintentos automáticos y disparador programado. | ✅ Completo |
| **C3 / O1** | **Gobernanza Unity Catalog** | Mínimo privilegio verificado con `SHOW GRANTS`, **Column Masking** para cédulas de proveedores, **Row Filter** por departamento y etiquetas `SET TAGS` de clasificación. | ✅ Completo |
| **C4** | **CI/CD con GitHub** | Git folder conectado, rama de trabajo, PR integrado y archivo **`databricks.yml` (Databricks Asset Bundles)** con targets `dev` y `prod`. | ✅ Completo |
| **C5** | **Observabilidad y Optimización** | **Liquid Clustering** por `(periodo_fiscal, categoria_gasto)`, ejecución de `OPTIMIZE`, evidencia en `DESCRIBE DETAIL` y consultas a `system.access.audit`. | ✅ Completo |
| **O7** | **Opcional: Ingesta desde S3** | Conexión e integración con recursos de la nube desde bucket AWS S3 (`nautilus-facturas01`). | ✅ Completo |

---

## 💡 4. Decisiones de Diseño y Justificación Técnica

### 1. ¿Por qué Auto Loader (`cloudFiles`) y no `COPY INTO`?
Auto Loader escala a millones de archivos sin degradación de rendimiento. Gracias al directorio de puntos de control (`_checkpoints`), mantiene el registro de los *offsets* y *commits* de los archivos descubiertos, permitiendo un procesamiento **idempotente y continuo** (*Exactly-Once Processing*), detectando además automáticamente cambios en el esquema (*Schema Evolution*).

### 2. ¿Por qué deduplicación por `clave_hacienda` con `QUALIFY ROW_NUMBER()`?
La clave de 50 caracteres emitida bajo el formato tributario de Costa Rica es la **llave natural unívoca** de cada comprobante. Ante reintentos de red o envíos dobles de facturas por parte de proveedores, la ventana analítica particionada por `clave_hacienda` y ordenada por `fecha_ingesta DESC` asegura de forma determinista conservar la última versión procesada.

### 3. ¿Por qué Tabla Delta en Gold en lugar de Vista o Materialized View?
En Databricks Serverless interactivo dentro de notebooks, las Materialized Views requieren un SQL Warehouse dedicado o un pipeline declarativo. Una tabla física Delta garantiza:
- **Enforzamiento de Constraints:** Solo las tablas físicas admiten restricciones `CHECK` que bloquean escrituras inválidas.
- **Bajo Costo de Lectura:** Los cálculos agregados (`SUM`, `AVG`) quedan precomputados físicamente, evitando recomputar en cada consulta analítica.
- **Soporte de Liquid Clustering:** Acelera la velocidad de filtrado para los tableros de control financiero.

### 4. ¿Por qué Liquid Clustering y no Particionado Tradicional Hive?
El particionado tradicional mediante carpetas (`/periodo_fiscal=2026-10/`) genera el problema de micro-archivos cuando el volumen por lote es pequeño. **Liquid Clustering** distribuye los datos multidimensionalmente utilizando curvas de espacio de Hilbert, lo que permite a Spark omitir la lectura de archivos irrelevantes (*Data Skipping*) sin rigidez estructural y permitiendo redefinir las columnas de agrupamiento en caliente.

---

## 🛠️ 5. Guía de Ejecución Paso a Paso ("Cómo se Corre")

### Prerrequisitos
- Workspace activo de **Databricks Free Edition**.
- Catálogo Unity Catalog habilitado.

### Secuencia de Ejecución de Notebooks:
1. **Paso 1: `00_config.py`**
   - Ejecuta la celda 1 para crear el catálogo `pf_avalos`, los esquemas `bronze`, `silver`, `gold` y el volume `landing`.
   - Ejecuta la celda 2 para crear la tabla dimensional `silver.cat_proveedores`.
   - Deja el widget en **`lote = 1`** y corre la celda 3 para depositar el primer lote de facturas en el Volume.
2. **Paso 2: `01_bronze.py` (Primera Ingesta)**
   - Corre el notebook completo. Auto Loader procesará las facturas del `lote_1`.
3. **Paso 3: Carga del Lote 2 y Prueba Incremental**
   - Regresa a `00_config.py`, cambia el widget a **`lote = 2`** y corre la celda 3.
   - Vuelve a `01_bronze.py` y córrelo de nuevo.
   - Revisa la celda de evidencia: verás que cada lote aparece exactamente una vez con su hora de procesamiento correspondiente.
4. **Paso 4: `02_silver.sql`**
   - Corre el notebook. Validarás la limpieza, el `JOIN` con proveedores, la normalización a colones y la celda comparativa de métricas antes/después.
5. **Paso 5: `03_gold.sql`**
   - Corre el notebook. Verás la tabla agregada y las restricciones `CHECK`. La celda de prueba con inserción negativa fallará con el error en rojo exigido como evidencia.
6. **Paso 6: `04_gobernanza.sql`**
   - Corre el notebook. Se aplicarán los permisos, el **Column Masking** en la cédula del emisor y el **Row Filter** por departamento.
7. **Paso 7: `05_observabilidad.sql`**
   - Corre el notebook para ejecutar `OPTIMIZE` con Liquid Clustering e inspeccionar las consultas a las System Tables.

---

## 👤 Autor
**Johan Ávalos Campos**  
*Data Engineer & Cloud Architecture Enthusiast*  
[Repositorio Oficial GitHub](https://github.com/johanavalosc/databricks-facturas-s3-pipeline)

# 🎙️ Guion Maestro y Discurso Técnico de Presentación
> **Proyecto Final:** Enterprise B2B Expense & Fiscal Audit Lakehouse  
> **Alumno:** Johan Ávalos Campos  
> **Profesor:** Andrés Calvo Vargas · Universidad Latina de Costa Rica  
> **Módulo 4:** Gestión y Gobernanza de Datos con Unity Catalog  

---

## 🧭 Orden de Navegación por Pestañas (De Izquierda a Derecha)

| Pestaña | Entorno | Qué se muestra en pantalla | Tema Evaluado |
| :---: | :--- | :--- | :--- |
| **1** | **GitHub** (`README.md`) | Diagrama visual SVG, problema de negocio y origen S3 (O7). | Presentación y Arquitectura |
| **2** | **Databricks** (`01_bronze`) | Auto Loader, inferencia, checkpoints y tabla de los 2 lotes. | Columna Vertebral (V1 Ingesta) |
| **3** | **Databricks** (`02_silver`) | Casting DECIMAL(18,2), tipo de cambio 520, deduplicación 50 dígitos, JOIN proveedores y métricas UNION ALL. | Columna Vertebral (V2 Limpieza) |
| **4** | **Databricks** (`03_gold`) | Reporte ejecutivo con subtotales/porcentajes, CHECK constraints y celda con error rojo de fila rechazada. | Componente 1 (Gold y Calidad) |
| **5** | **Databricks** (`04_gobernanza`) | Mínimo privilegio SHOW GRANTS, función mask_cedula, tags de auditoría y Liquid Clustering con DESCRIBE DETAIL. | Componentes 3 y 5 (Gobernanza y Optimización) |
| **6** | **Databricks** (Lakeflow Job) | Grafo DAG en pestaña Tasks (Serverless, reintentos) y corrida oficial Succeeded en pestaña Runs. | Componente 2 (Orquestación) |
| **7** | **GitHub** (Pull Request & CI/CD) | Rama feature integrada a main por PR #1 y databricks.yml (DABs dev/prod). | Componente 4 (CI/CD) |

---

## 🗣️ Discurso Completo Paso a Paso (Para Hablar con Fluidez y Propiedad)

---

### 🌐 PESTAÑA 1: GitHub (`README.md` y Diagrama de Arquitectura)
*(Acción: Compartes pantalla en el README de tu repositorio GitHub. Muestras el título y el diagrama visual SVG)*

> **Lo que dices:**  
> *"Buenas tardes, profesor. Al iniciar la llamada le compartí en el chat el enlace directo a mi repositorio de GitHub para que pueda auditar todo el código y la documentación técnica.*  
>  
> *Para este proyecto final elegí resolver un problema real de ingeniería de datos y finanzas corporativas aquí en Costa Rica: el **control, homologación y auditoría fiscal de facturas electrónicas B2B sobre una arquitectura Lakehouse en Databricks**.*  
>  
> *El problema de negocio es clarísimo: en cualquier empresa mediana o grande, las compras a proveedores llegan de forma caótica. Nos mandan comprobantes en formato XML y JSON, unos en colones y otros en dólares. Esto genera tres problemas operativos graves:*  
> *1. **Riesgo de pago doble:** Si un proveedor reenvía una factura por un fallo de correo o red, se corre el riesgo de duplicar el pago contable.*  
> *2. **Dispersión cambiaria:** Al tener facturas en dólares sin homologar, los cierres contables sufren por volatilidad del tipo de cambio.*  
> *3. **Pérdida de crédito fiscal:** El Ministerio de Hacienda exige calcular con exactitud quirúrgica el IVA soportado para deducirlo en el formulario D-104.*  
>  
> *(Aquí señalas el diagrama visual SVG en pantalla)*  
>  
> *Hablemos de la arquitectura técnica y el origen de los datos:*  
> *Para cumplir con el **tema opcional O7** que acordamos en clase, los datos no son inventados: provienen de facturas electrónicas reales bajo el formato v4.4 de Hacienda, almacenadas en un bucket privado de **AWS S3 llamado `nautilus-facturas01`**.*  
>  
> *¿Dónde y cómo guardamos las cosas en Databricks?*  
> *En Databricks organizamos todo bajo el **namespace de tres niveles de Unity Catalog**: el catálogo se llama **`pf_avalos`**, y adentro creamos tres esquemas por capa: **`bronze`**, **`silver`** y **`gold`**.*  
> *Como estamos en Databricks Free Edition, la plataforma tiene bloqueada la creación de External Locations con IAM de AWS a nivel de cuenta (tal como usted lo indicó en la página 5 de la guía). Por eso, para la zona de aterrizaje creamos un **Volume administrado de Unity Catalog** ubicado en:  
> `/Volumes/pf_avalos/bronze/landing/`.*  
> *Ahí sincronizamos las facturas desde S3 en carpetas por lotes (`lote_1/` y `lote_2/`). A partir de ese Volume, montamos el pipeline Medallion completo. Vamos a verlo directamente en el workspace."*

---

### 🌐 PESTAÑA 2: Databricks Notebook `01_bronze` (Ingesta Incremental)
*(Acción: Haces clic en la Pestaña 2. Haces scroll pausado mostrando el código y te detienes en la celda de la tabla por lote)*

> **Lo que dices:**  
> *"Estamos aquí en el notebook de Bronze, que cubre la primera parte de la columna vertebral y la Sección 2 del examen de certificación.*  
>  
> *¿Qué tipo de ingesta tenemos y cómo la hacemos?*  
> *No usamos una ingesta tradicional por batch con `spark.read` ni usamos `COPY INTO`. Implementamos **Auto Loader de Databricks utilizando `spark.readStream.format("cloudFiles")`** con la opción `cloudFiles.format = "json"`.*  
>  
> *¿Por qué Auto Loader y qué ventajas técnicas tiene?*  
> *1. **Inferencia y evolución de esquemas:** Le configuramos la propiedad `cloudFiles.schemaLocation` apuntando a `/Volumes/pf_avalos/bronze/landing/_schema/`. Con esto, Spark lee los JSON semiestructurados de las facturas, infiere automáticamente los tipos de datos y, si el día de mañana Hacienda agrega un campo nuevo a la factura, Auto Loader lo asimila sin romper el pipeline.*  
> *2. **Mecanismo de Checkpoint e Idempotencia:** Le definimos un directorio de control en:  
> `/Volumes/pf_avalos/bronze/landing/_checkpoints/facturas_bronze`.*  
> *El checkpoint guarda en almacenamiento persistente los offsets y el log transaccional de los archivos que ya descubrió y procesó. Esto nos da una garantía estricta de procesamiento **Exactly-Once**.*  
> *3. **Disparador `AvailableNow`:** Como estamos en un entorno Serverless por lotes, configuramos `.trigger(availableNow=True)`. Esto levanta el flujo de streaming, procesa incrementalmente todos los archivos nuevos que hayan caído en el Volume, actualiza los checkpoints y se apaga de inmediato, consumiendo el mínimo de cómputo.*  
>  
> *¿Dónde se guardan los datos procesados?*  
> *Se escriben en modo append sobre una tabla Delta física: **`pf_avalos.bronze.facturas_raw`**.*  
> *Para cumplir con la trazabilidad que pide el examen, enriquecimos la ingesta extrayendo la columna del sistema `_metadata.file_path` como `archivo_origen` y le agregamos `current_timestamp()` como `fecha_ingesta`.*  
>  
> *(Pones el mouse sobre la tabla con las filas de Lote 1 y Lote 2)*  
>  
> *Vea la celda de evidencia, profesor:*  
> *Hicimos una prueba real cargando dos tandas:*  
> *- El **Lote 1**, con 5 facturas (AWS en dólares, Estación Delta en colones, Repuestos La Central, Office Depot en dólares y el ICE).*  
> *- Luego depositamos el **Lote 2**, con 3 facturas (Cooperativa Dos Pinos, CISCO en dólares y Megasuper).*  
> *Aquí en la consulta agrupada por lote se ve la prueba matemática: el Lote 1 tiene 5 filas con una marca de tiempo inicial, y el Lote 2 tiene 3 filas con una hora posterior. **Cada lote aparece exactamente una sola vez**.*  
> *Si yo vuelvo a ejecutar esta ingesta en este mismo instante sin meter archivos nuevos a S3, Auto Loader lee el checkpoint, detecta 0 archivos pendientes y no duplica absolutamente nada."*

---

### 🌐 PESTAÑA 3: Databricks Notebook `02_silver` (Limpieza y Homologación)
*(Acción: Haces clic en la Pestaña 3. Muestras la consulta SQL de transformación y bajas a la tabla comparativa de métricas)*

> **Lo que dices:**  
> *"Pasamos a la capa Silver, que completa la columna vertebral y cubre la Sección 3 del examen.*  
> *En Bronze tenemos 8 facturas crudas en formato texto y con monedas heterogéneas. La misión de Silver es aplicar ingeniería de datos rigurosa para entregar una tabla limpia, deduplicada y lista para reportería contable en **`pf_avalos.silver.facturas_validadas`**.*  
>  
> *Aquí aplicamos cuatro transformaciones técnicas fundamentales:*  
>  
> *1. **Casting estricto y saneamiento de nulos:** Parseamos los strings a tipos nativos: fechas a `DATE`, y los importes financieros a **`DECIMAL(18,2)`**. Esto es vital en ingeniería de datos financieros porque usar `FLOAT` o `DOUBLE` provocaría errores de redondeo por representación binaria.*  
>  
> *2. **Normalización cambiaria (USD a CRC):** En Costa Rica la contabilidad oficial se presenta en colones. Implementamos una lógica condicional donde evaluamos la columna `moneda`: si viene en `'USD'`, multiplicamos el subtotal, el impuesto y el total por el tipo de cambio oficial de **520 colones por dólar**; si viene en `'CRC'`, mantenemos el monto original. Ahora el 100% de la tabla habla en la misma moneda base.*  
>  
> *3. **Deduplicación determinista por llave de negocio:** En el sistema tributario costarricense, cada factura tiene una **clave numérica única de 50 dígitos** emitida por Hacienda. Para eliminar duplicados no usamos un `DISTINCT` genérico; utilizamos la función analítica de ventana:  
> `QUALIFY ROW_NUMBER() OVER (PARTITION BY clave_hacienda ORDER BY fecha_ingesta DESC) = 1`.*  
> *¿Por qué esto es superior? Porque si un proveedor envía una corrección de factura el mismo día, esta ventana particiona por la clave, ordena por la hora de llegada y conserva determinísticamente la última versión válida sin disparar un shuffle masivo en Spark.*  
>  
> *4. **Enriquecimiento relacional (Master Data Management):** Hicimos un `INNER JOIN` con nuestra tabla dimensional **`pf_avalos.silver.cat_proveedores`**, cruzando por la cédula jurídica del emisor. Así le inyectamos a cada factura su centro de costos, el departamento responsable (Tecnología, Operaciones, Mantenimiento) y la categoría de gasto homologada.*  
>  
> *(Señalas la tabla de métricas antes y después con el `UNION ALL`)*  
>  
> *En esta celda comparativa demostramos la integridad del pipeline: teníamos 8 facturas en Bronze y consolidamos exactamente 8 registros únicos en Silver, con 8 claves fiscales distintas, cero duplicados y cero nulos en campos obligatorios."*

---

### 🌐 PESTAÑA 4: Databricks Notebook `03_gold` (Reporte Ejecutivo, Calidad y Error)
*(Acción: Haces clic en la Pestaña 4. Muestras la nueva tabla de Reporte Ejecutivo y luego bajas despacio al bloque rojo con el error)*

> **Lo que dices:**  
> *"Llegamos al Componente 1 del examen: la capa Gold y el aseguramiento de calidad de datos.*  
>  
> *Aquí creamos la tabla física: **`pf_avalos.gold.gastos_por_departamento`**.*  
> *Esta tabla responde una pregunta de negocio estratégica: **¿cuánto presupuesto ejecutó cada departamento de la empresa desglosado por categoría, y cuánto crédito fiscal de IVA acumulamos para reportar en el formulario D-104 de Hacienda?***  
>  
> *¿Por qué elegí una tabla Delta física en vez de una vista?*  
> *Primero, porque en reportes analíticos consumidos por Power BI o dashboards de Databricks, una tabla física persiste las agregaciones (`SUM`, `COUNT`) precomputadas, eliminando la latencia de recomputar millones de registros. Y segundo, porque las vistas no soportan restricciones físicas de integridad.*  
>  
> *(Pones el mouse sobre la tabla del Reporte Ejecutivo)*  
>  
> *Mire esta consulta ejecutiva que armé con funciones de ventana (`OVER ()`) y `UNION ALL`:*  
> *Aquí no solo vemos el gasto plano, sino que calculamos el impacto contable real:*  
> *- **Tecnología** ejecutó el **47.3% del presupuesto corporativo** (casi un millón de colones en infraestructura Cloud de AWS y Cisco).*  
> *- **Operaciones** ejecutó el **25.3%** en repuestos y mantenimiento.*  
> *- Y calculamos con exactitud que la empresa tiene un **crédito fiscal deducible de IVA de ₡216,251.75 colones** a favor ante Hacienda.*  
> *- Abajo, la fila de **`TOTAL EMPRESA`** demuestra el cuadre contable perfecto: 8 facturas procesadas por un total de **₡2,107,026.75 colones**.*  
>  
> *¿Cómo forzamos la calidad de datos físicamente?*  
> *Utilizamos **CHECK constraints nativos de Delta Lake**:*  
> *- `ALTER TABLE ... ADD CONSTRAINT gasto_no_negativo CHECK (gasto_total >= 0)`*  
> *- `ALTER TABLE ... ADD CONSTRAINT iva_no_negativo CHECK (credito_fiscal_iva >= 0)`*  
>  
> *(Pones el cursor sobre el recuadro rojo con el error)*  
>  
> *Y mire aquí la evidencia literal que solicita la rúbrica: ejecuté a propósito un `INSERT` manual con una fila corrupta intentando meter un gasto de **-50,000 colones y un IVA negativo**.*  
> *Como puede ver en la salida de la celda, el motor transaccional de Databricks abortó la operación inmediatamente lanzando el error: **`DELTA_VIOLATE_CONSTRAINT_WITH_VALUES: CHECK constraint gasto_no_negativo violated`**.*  
> *Esto demuestra que gracias a las propiedades ACID de Delta Lake, los datos corruptos jamás entran a contaminar la capa Gold de la empresa."*

---

### 🌐 PESTAÑA 5: Databricks Notebook `04_gobernanza` (Seguridad y Gobernanza)
*(Acción: Haces clic en la Pestaña 5. Muestras `SHOW GRANTS`, la función `mask_cedula`, los tags y `DESCRIBE DETAIL`)*

> **Lo que dices:**  
> *"En la Pestaña 5 abordamos el Componente 3 de Gobernanza con Unity Catalog y el Componente 5 de Optimización.*  
>  
> *En gobernanza aplicamos tres niveles de protección:*  
> *1. **Principio de Mínimo Privilegio:** Como se ve en la celda de `SHOW GRANTS`, a los usuarios corporativos (`account users`) no se les otorgan privilegios administrativos; únicamente se les concede de forma granular `USE CATALOG` en `pf_avalos`, `USE SCHEMA` en `gold` y `silver`, y `SELECT` de lectura sobre las tablas que necesitan.*  
> *2. **Column Masking dinámico:** La cédula jurídica de los proveedores es un dato sensible. Implementamos una función UDF de enmascaramiento llamada `mask_cedula` asociada a una tabla de mapeo de roles (`silver.seguridad_mapeo_roles`). Si un usuario del equipo financiero consulta la tabla, ve la cédula completa; si entra cualquier otro usuario o analista general, la cédula se anonimiza automáticamente en formato `••••-••••-1234`.*  
> *3. **Clasificación y Auditoría con Tags:** Usamos sentencias `ALTER TABLE ... ALTER COLUMN ... SET TAGS` para clasificar las columnas en el catálogo de metadatos: le pusimos la etiqueta `'PII' = 'Confidencial'` a la cédula del proveedor y `'Dominio' = 'Financiero'` al total en colones.*  
> *(Señalas la tabla de tags)*  
> *Aquí en la consulta sobre `system.information_schema.column_tags` se demuestra que Unity Catalog audita y expone estas etiquetas a nivel de gobierno corporativo.*  
>  
> *Por último, en cuanto a **Optimización y Observabilidad (Componente 5)**:*  
> *En lugar de utilizar particionado tradicional de carpetas (`PARTITIONED BY`), que genera el problema clásico de micro-archivos cuando las particiones son pequeñas, aplicamos **Liquid Clustering** sobre la tabla Silver ejecutando:  
> `ALTER TABLE pf_avalos.silver.facturas_validadas CLUSTER BY (periodo_fiscal, categoria_gasto)`  
> *y corrimos `OPTIMIZE`.*  
> *Liquid Clustering organiza los datos internamente mediante curvas de espacio de Hilbert, lo que permite a Databricks hacer un **Data Skipping multidimensional ultra rápido** cuando finanzas filtra por mes o por categoría.*  
> *Y en la salida de `DESCRIBE DETAIL`, se comprueba en los metadatos que la propiedad `clusteringColumns` quedó formalmente configurada."*

---

### 🌐 PESTAÑA 6: Databricks - Lakeflow Jobs (`Tasks` y `Runs`)
*(Acción: Haces clic en la Pestaña 6. Primero muestras la pestaña `Tasks` con el DAG, y luego haces clic en `Runs`)*

> **Lo que dices:**  
> *(Señalas el grafo visual del DAG con las 3 tareas)*  
> *"En el Componente 2 de Orquestación automatizamos todo el flujo mediante un **Lakeflow Job** llamado `[PROD] Lakeflow Pipeline Facturacion B2B - Medallion`.*  
>  
> *Diseñamos un **DAG multi-tarea con dependencias lineales**:*  
> *- La tarea `01_bronze_ingesta` procesa los archivos desde S3.*  
> *- La tarea `02_silver_limpieza` tiene dependencia directa de Bronze mediante `depends_on`, garantizando que solo ejecute si Bronze finalizó con éxito.*  
> *- Y la tarea `03_gold_analitica` depende estrictamente del éxito de Silver.*  
>  
> *Puntos técnicos clave de la orquestación:*  
> *1. **Cómputo Serverless:** Todo el job se ejecuta sobre Serverless Compute. Esto elimina los 4 o 5 minutos que tarda un cluster tradicional en encender máquinas virtuales y reduce drásticamente el costo operativo.*  
> *2. **Resiliencia y Reintentos:** En la tarea Bronze configuramos `max_retries = 2` con un intervalo de reintento de 10 segundos. Si ocurre cualquier caída temporal de red al consultar el bucket de S3, el pipeline se auto-recupera automáticamente.*  
> *3. **Disparador programado:** Le configuramos una expresión Quartz Cron (`0 0 6 * * ?`) programada para correr todos los días a las 6:00 AM hora de Costa Rica.*  
>  
> *(Haces clic en la sub-pestaña **Runs** arriba a la izquierda)*  
>  
> *Y aquí en pantalla puede ver la evidencia de producción: la corrida oficial de producción, la cual ejecutó el DAG de punta a punta con éxito total (**`Succeeded`**) en apenas **47 segundos**, con las tres tareas terminadas en verde impecable."*

---

### 🌐 PESTAÑA 7: GitHub (Pull Request & CI/CD)
*(Acción: Haces clic en la Pestaña 7 en GitHub, en el Pull Request #1 cerrado y mergeado)*

> **Lo que dices:**  
> *"Para cerrar con el Componente 4 de CI/CD, todo el código del Lakehouse vive versionado en este repositorio de GitHub conectado directamente a Databricks mediante Git Folders.*  
>  
> *Siguiendo las buenas prácticas profesionales de ingeniería de software:*  
> *1. No se desarrolló directamente sobre la rama principal `main`. Se creó una rama de características llamada **`feature/pipeline-medallion-s3`**.*  
> *2. Los notebooks y configuraciones se trabajaron de forma modular, se hicieron commits descriptivos y se integraron formalmente a `main` a través de este **Pull Request #1**, dejando un historial de auditoría y trazabilidad completo.*  
> *3. Además, en la raíz del proyecto incluimos el archivo **`databricks.yml` utilizando Databricks Asset Bundles (DABs)**. Con DABs definimos la infraestructura como código, declarando los entornos `dev` y `prod` con sus respectivos targets y permisos para desplegar el job de forma automatizada mediante pipelines de CI/CD.*  
>  
> *En conclusión, profesor:*  
> *Cumplimos al 100% la columna vertebral demostrando ingesta incremental y limpieza; cubrimos los 5 componentes evaluados del examen Data Engineer Associate; integramos el tema opcional O7 con AWS S3; y demostramos cómo gobernar y optimizar un Lakehouse moderno listo para producción.*  
>  
> *Muchas gracias por su atención y quedo a su total disposición para responder la pregunta técnica del examen."*

---

## 🎯 Cheat-Sheet Rápido de Preguntas de Examen

1. **Si te pregunta por AWS S3:**  
   * *"Profe, como estamos en Databricks Free Edition, la plataforma tiene bloqueada la creación de External Locations y Storage Credentials con IAM de AWS (tal como usted lo indicó en la guía). Por eso los datos reales viven en mi bucket S3 `nautilus-facturas01`, y para la ingesta los sincronizamos al Volume de Unity Catalog, donde Auto Loader los procesa de forma incremental con checkpoints."*

2. **Si te pregunta: *"¿Por qué Auto Loader y no COPY INTO?"***  
   * *"Auto Loader con `cloudFiles` escala eficientemente a millones de archivos porque utiliza directorios de checkpoints y seguimiento de offsets para procesar de forma incremental. `COPY INTO` tiene que listar recursivamente todo el bucket de almacenamiento en cada corrida, lo cual se vuelve costoso y lento a medida que crece el histórico."*

3. **Si te pregunta: *"¿Por qué Liquid Clustering y no particionado clásico?"***  
   * *"El particionado tradicional (`PARTITIONED BY`) crea carpetas rígidas en el disco y genera problemas de micro-archivos cuando el volumen es pequeño. Liquid Clustering co-localiza los datos multidimensionalmente usando curvas de espacio de Hilbert dentro de los archivos Parquet, permite cambiar las columnas de agrupamiento en caliente sin reescribir la tabla, y habilita Data Skipping en varias columnas al mismo tiempo."*

4. **Si te pregunta: *"¿Qué pasa con los permisos si un usuario tiene SELECT pero no tiene USE CATALOG ni USE SCHEMA?"***  
   * *"En Unity Catalog la jerarquía de 3 niveles es obligatoria. Si el usuario no tiene `USE CATALOG` y `USE SCHEMA`, el motor no puede resolver la ruta hacia la tabla y le bloquea el acceso aunque tenga permiso de `SELECT` sobre el objeto."*

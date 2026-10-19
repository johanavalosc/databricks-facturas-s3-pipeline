# 🎙️ Guion Maestro de Presentación en Vivo (20 Minutos)
> **Proyecto Final:** Enterprise B2B Expense & Fiscal Audit Lakehouse  
> **Alumno:** Johan Ávalos Campos  
> **Profesor:** Andrés Calvo Vargas · Universidad Latina de Costa Rica  

---

## ⏱️ Distribución del Tiempo (Total: 18 - 20 minutos)

| Bloque | Minutos | Qué mostrar en pantalla | Qué decir (Speech) |
| :--- | :---: | :--- | :--- |
| **1. Introducción** | 0:00 - 2:00 | Tu repositorio de GitHub y el `README.md` | Saludo, problema corporativo de facturas y origen en S3. |
| **2. Arquitectura** | 2:00 - 4:00 | Diagrama Mermaid en el README | Explicar el flujo S3 $\to$ Volume $\to$ Medallion $\to$ Governance. |
| **3. Columna Vertebral** | 4:00 - 9:00 | Databricks Workspace (`01_bronze.py` y `02_silver.sql`) | Mostrar la ingesta incremental (Lote 1 y 2) y las métricas de limpieza Silver. |
| **4. Componentes y Evidencias** | 9:00 - 15:00 | Notebooks `03_gold`, `04_gobernanza`, `05_observabilidad` y Job | Error en rojo de la fila rechazada, Column Masking, Liquid Clustering y DAG de Lakeflow. |
| **5. CI/CD y Repositorio** | 15:00 - 17:00 | GitHub (Pull Request integrado a main y `databricks.yml`) | Mostrar cómo el código vive versionado con Databricks Asset Bundles. |
| **6. Cierre y Preguntas** | 17:00 - 20:00 | Cámara / Micrófono abierto | Qué aprendiste para el examen y responder la pregunta del docente. |

---

## 🗣️ Guion Detallado Paso a Paso (Lo que vas a decir y mostrar)

### Minuto 0:00 - 2:00 · El Problema y los Datos
> *"Buenas tardes, profesor. Al iniciar la llamada le compartí por el chat el enlace a mi repositorio público de GitHub.*  
> *Mi proyecto aborda un problema crítico en finanzas: el control de facturas de compras B2B y la auditoría tributaria.*  
> *En una empresa, las facturas llegan en múltiples formatos y monedas (dólares y colones), con riesgo de pagos duplicados y pérdida del crédito fiscal de IVA ante Hacienda.*  
> *Los datos provienen de facturas electrónicas reales de Costa Rica almacenadas en un bucket privado de **AWS S3 (`nautilus-facturas01`)**, cumpliendo con el tema opcional O7 que acordamos en clase, sincronizadas hacia un Volume administrado en Unity Catalog."*

---

### Minuto 2:00 - 4:00 · La Arquitectura
> *(Muestras el diagrama de arquitectura en el README)*  
> *"La arquitectura sigue el patrón Medallion:*  
> *1. Las facturas aterrizan en el Volume `pf_avalos.bronze.landing`.*  
> *2. En Bronze, Auto Loader ingesta por lotes garantizando procesamiento exactly-once.*  
> *3. En Silver, normalizamos los montos a colones (CRC) con el tipo de cambio oficial, deduplicamos por la clave fiscal de 50 dígitos y hacemos un JOIN con el catálogo de proveedores.*  
> *4. En Gold, calculamos el crédito fiscal deducible y aplicamos restricciones CHECK.*  
> *5. Todo gobernado con Column Masking para cédulas jurídicas, Liquid Clustering y orquestado con Lakeflow Jobs."*

---

### Minuto 4:00 - 9:00 · La Columna Vertebral Funcionando (En Vivo)
> *(Pasas al workspace de Databricks)*  
> 1. **Mostrar `00_config.py`:**  
>    *"Aquí creé mi catálogo `pf_avalos`, los esquemas por capa y el catálogo de proveedores.*  
> 2. **Mostrar `01_bronze.py` (Evidencia Incremental):**  
>    *(Muestras la celda de la consulta agrupada por lote)*  
>    *"Mire, profesor: cargamos el Lote 1 con 5 facturas y luego el Lote 2 con 3 facturas. Cada lote aparece exactamente una sola vez, con su propia hora de ingesta. Si vuelvo a correr esta celda sin archivos nuevos, el checkpoint registra que no hay nada pendiente y no duplica filas."*  
> 3. **Mostrar `02_silver.sql` (Métricas Antes y Después):**  
>    *(Muestras la celda con el `UNION ALL`)*  
>    *"En Silver aplicamos un tipado estricto y un JOIN con `silver.cat_proveedores`. En esta celda comparativa vemos cómo en Bronze teníamos datos con monedas mixtas y montos nominales sin homologar, y en Silver quedan consolidados en colones con 0 nulos en la clave del negocio."*

---

### Minuto 9:00 - 15:00 · Los Componentes del Examen y sus Evidencias

1. **Capa Gold y Reglas de Calidad (`03_gold.sql`):**  
   - Muestras la tabla analítica: *"Esta tabla responde cuánto gastó cada departamento y cuánto IVA podemos deducir en el formulario D-104 de Hacienda"*.  
   - Muestras la celda con el error rojo: *"Aquí apliqué dos constraints CHECK. Esta celda que ve en rojo intentó insertar un gasto con monto negativo: Databricks la rechazó inmediatamente violando la regla `gasto_total_positivo`"*.
2. **Gobernanza (`04_gobernanza.sql`):**  
   - Muestras `SHOW GRANTS`: *"Aplicamos mínimo privilegio concediendo únicamente `USE CATALOG`, `USE SCHEMA` y `SELECT` a nivel de cuenta"*.  
   - Muestras el **Column Masking**: *"Mire la columna `proveedor_cedula`: para perfiles no financieros, la cédula se anonimiza automáticamente a `••••-••••-1234` mediante una función con tabla de mapeo"*.  
   - Muestras las etiquetas `SET TAGS` de clasificación PII y tributaria.
3. **Observabilidad (`05_observabilidad.sql`):**  
   - Muestras `DESCRIBE DETAIL`: *"En lugar de particionado rígido tradicional, aplicamos **Liquid Clustering** por `periodo_fiscal` y `categoria_gasto`, y corrimos `OPTIMIZE` para compactar archivos y maximizar el Data Skipping"*.  
   - Muestras la consulta a `system.access.audit` con los eventos recientes sobre el catálogo.
4. **Lakeflow Job (Orquestación):**  
   - Abres la pestaña de **Workflows / Jobs** en Databricks: *"Aquí está mi Job configurado con las 3 tareas en DAG (`bronze` $\to$ `silver` $\to$ `gold`), con reintentos automáticos y una corrida exitosa verde completa"*.

---

### Minuto 15:00 - 17:00 · CI/CD con GitHub
> *(Muestras tu repositorio de GitHub)*  
> *"Todo el proyecto vive en un repositorio conectado mediante Databricks Git Folders.*  
> *Trabajé en una rama `feature/financial-pipeline` e integré los cambios a `main` a través de un Pull Request.*  
> *Además, incluí el archivo `databricks.yml` utilizando **Databricks Asset Bundles (DABs)** para desplegar los jobs automáticamente en entornos dev y prod."*

---

### Minuto 17:00 - 20:00 · Cierre y Preguntas del Examen
> *"Para concluir, este proyecto me permitió practicar con mis propias manos los pilares del examen Data Engineer Associate: la idempotencia de Auto Loader, la integridad referencial en Delta, la gobernanza granular de Unity Catalog y la optimización con Liquid Clustering.*  
> *Quedo a su disposición para cualquier pregunta técnica."*

---

## 🎯 Cheat-Sheet: Preguntas Típicas del Docente y Respuestas Exactas

### Pregunta 1: *"¿Por qué usaste Auto Loader y no COPY INTO?"*
- **Tu respuesta:** *"Auto Loader con `cloudFiles` escala eficientemente a millones de archivos porque utiliza notificaciones de eventos en la nube y directorios de checkpoints para rastrear de forma incremental qué archivos ya fueron procesados. `COPY INTO` tiene que listar recursivamente todo el bucket de almacenamiento en cada corrida, lo cual se vuelve costoso y lento a medida que crece el histórico."*

### Pregunta 2: *"¿Qué diferencia hay entre Liquid Clustering y el particionado clásico con PARTITIONED BY?"*
- **Tu respuesta:** *"El particionado tradicional crea subcarpetas rígidas en el almacenamiento. Si las particiones tienen pocos datos, crea el problema de micro-archivos (*small files*). Liquid Clustering utiliza curvas de espacio de Hilbert para co-localizar los datos físicamente dentro de los archivos Parquet, permite cambiar las columnas de clustering en cualquier momento sin reescribir toda la tabla y habilita Data Skipping eficiente en múltiples dimensiones al mismo tiempo."*

### Pregunta 3: *"¿Qué pasa con los permisos si un usuario tiene SELECT en la tabla pero no tiene USE CATALOG ni USE SCHEMA?"*
- **Tu respuesta:** *"Unity Catalog aplica una jerarquía estricta de 3 niveles. Si el usuario no tiene `USE CATALOG` en el catálogo y `USE SCHEMA` en el esquema, no puede resolver la ruta del objeto ni navegar hacia la tabla, por lo que el acceso falla aunque tenga permiso de `SELECT` sobre la tabla."*

### Pregunta 4: *"¿Por qué dejaste una celda fallada en la capa Gold?"*
- **Tu respuesta:** *"Porque las restricciones CHECK en Delta Lake son una garantía física de integridad de datos. La celda fallada demuestra que cuando un pipeline o un usuario intenta insertar un registro con montos negativos inconsistentes, el motor transaccional de Databricks bloquea la escritura para proteger los reportes contables."*

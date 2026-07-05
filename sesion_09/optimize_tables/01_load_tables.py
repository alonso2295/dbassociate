# Databricks notebook source
# MAGIC %md
# MAGIC # Control Table: `dbassociate.ctl_tb.lakehouse_tables`
# MAGIC
# MAGIC ## Objetivo
# MAGIC Este notebook construye y mantiene una **tabla de control** que centraliza qué
# MAGIC tablas del metastore de Unity Catalog deben someterse a rutinas de mantenimiento
# MAGIC (`OPTIMIZE` / `VACUUM`). El flujo es el siguiente:
# MAGIC
# MAGIC 1. Crear la tabla de control `dbassociate.ctl.optimize_tables` (si no existe).
# MAGIC 2. Leer el catálogo de tablas disponibles desde `system.information_schema.tables`.
# MAGIC 3. Sincronizar (`MERGE`) el catálogo de tablas leído contra la tabla de control:
# MAGIC    - Tablas nuevas en el origen → se **insertan** con valores por defecto.
# MAGIC    - Tablas que ya no existen en el origen pero sí en el control → se **desactivan**
# MAGIC      (`flg_active = 0`).
# MAGIC 4. Construir un DataFrame con únicamente las tablas activas (`flg_active = 1`).
# MAGIC 5. Repartir esas tablas de forma equitativa en 2 grupos (para, por ejemplo,
# MAGIC    paralelizar o alternar ventanas de mantenimiento).
# MAGIC 6. Agregar una columna con el nombre completo (`catalog.schema.table`).
# MAGIC
# MAGIC ## Requisitos previos
# MAGIC - Permisos para crear el catálogo/esquema `dbassociate.ctl` (o que ya existan).
# MAGIC - Permisos de lectura sobre `system.information_schema.tables` (Unity Catalog).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parámetros del notebook

# COMMAND ----------

dbutils.widgets.text("catalog_name", "dbassociate", "Catálogo de control")
dbutils.widgets.text("schema_name", "ctl_tb", "Esquema de control")
dbutils.widgets.text("table_name", "lakehouse_tables", "Tabla de control")
dbutils.widgets.text("buckets", "2", "Numero de buckets")

CTL_CATALOG = dbutils.widgets.get("catalog_name")
CTL_SCHEMA  = dbutils.widgets.get("schema_name")
CTL_TABLE   = dbutils.widgets.get("table_name")
BUCKETS   = int(dbutils.widgets.get("buckets"))
FULL_CTL_TABLE = f"{CTL_CATALOG}.{CTL_SCHEMA}.{CTL_TABLE}"

print(f"Tabla de control -> {FULL_CTL_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Creación del catálogo, esquema y tabla de control
# MAGIC
# MAGIC Se define explícitamente el esquema de la tabla, incluyendo los valores por
# MAGIC defecto solicitados (`flg_vacuum`, `vacuum_retention_days`, `flg_optimize`,
# MAGIC `flg_active`).
# MAGIC
# MAGIC | Columna | Tipo | Default | Descripción |
# MAGIC |---|---|---|---|
# MAGIC | catalog_name | STRING | - | Catálogo de la tabla monitoreada |
# MAGIC | schema_name | STRING | - | Esquema de la tabla monitoreada |
# MAGIC | table_name | STRING | - | Nombre de la tabla monitoreada |
# MAGIC | flg_vacuum | INT | 1 | Indica si se debe ejecutar `VACUUM` |
# MAGIC | vacuum_retention_days | INT | 7 | Días de retención para `VACUUM` |
# MAGIC | flg_optimize | INT | 1 | Indica si se debe ejecutar `OPTIMIZE` |
# MAGIC | flg_active | INT | 1 | Indica si la tabla sigue existiendo en el origen |
# MAGIC | created_timestamp | TIMESTAMP | - | Fecha de alta del registro |
# MAGIC | updated_timestamp | TIMESTAMP | - | Fecha de última actualización del registro |
# MAGIC
# MAGIC Ejecutar el siguiente código por unica vez para crear la tabla
# MAGIC
# MAGIC ```
# MAGIC spark.sql(f"CREATE CATALOG IF NOT EXISTS {CTL_CATALOG}")
# MAGIC spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CTL_CATALOG}.{CTL_SCHEMA}")
# MAGIC
# MAGIC spark.sql(f"""
# MAGIC CREATE TABLE IF NOT EXISTS {FULL_CTL_TABLE} (
# MAGIC     catalog_name            STRING,
# MAGIC     schema_name              STRING,
# MAGIC     table_name                STRING,
# MAGIC     fqn_table                  STRING,  
# MAGIC     flg_vacuum                INT,
# MAGIC     vacuum_retention_days      INT,
# MAGIC     flg_optimize               INT,
# MAGIC     flg_active                 INT,
# MAGIC     created_timestamp          TIMESTAMP,
# MAGIC     updated_timestamp          TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Tabla de control para rutinas de mantenimiento (OPTIMIZE / VACUUM) sobre tablas de Unity Catalog'
# MAGIC """)
# MAGIC
# MAGIC print(f"Tabla {FULL_CTL_TABLE} verificada/creada correctamente.")
# MAGIC display(spark.sql(f"DESCRIBE TABLE {FULL_CTL_TABLE}"))
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Lectura del catálogo de tablas origen
# MAGIC
# MAGIC Se consulta `system.information_schema.tables`, la vista del sistema de Unity
# MAGIC Catalog que expone metadatos de todas las tablas visibles para el usuario actual.
# MAGIC Se excluyen los catálogos/esquemas de sistema para no incluir la propia tabla de
# MAGIC control ni objetos internos en el barrido.

# COMMAND ----------

from pyspark.sql import functions as F

df_source = (
    spark.table("system.information_schema.tables")
        .select(
            F.col("table_catalog").alias("catalog_name"),
            F.col("table_schema").alias("schema_name"),
            F.col("table_name"),
            F.concat_ws(".", F.col("catalog_name"), F.col("schema_name"), F.col("table_name")).alias("fqn_table")
        )
        .where(
            (F.col("catalog_name").isin("dbassociate"))
            & (F.col("schema_name").isin("bronze", "silver", "gold"))
        )
        .distinct()
)

print(f"Total de tablas candidatas encontradas en el origen: {df_source.count()}")
display(df_source)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Sincronización (`MERGE`) hacia la tabla de control
# MAGIC
# MAGIC Reglas del `MERGE` (usando `catalog_name + schema_name + table_name` como clave):
# MAGIC
# MAGIC - **`WHEN NOT MATCHED BY TARGET`** → la tabla existe en el origen pero no en el
# MAGIC   control → se **inserta** con los valores por defecto y `flg_active = 1`.
# MAGIC - **`WHEN NOT MATCHED BY SOURCE`** → la tabla existe en el control pero ya no en
# MAGIC   el origen → se **desactiva** (`flg_active = 0`) y se actualiza `updated_timestamp`.
# MAGIC - No se define una cláusula `WHEN MATCHED` porque no se solicita actualizar filas
# MAGIC   que siguen existiendo en ambos lados; si se requiriera "reactivar" filas que
# MAGIC   vuelven a aparecer en el origen, se podría añadir un `WHEN MATCHED` que fuerce
# MAGIC   `flg_active = 1` (queda comentado como referencia).

# COMMAND ----------

# Creamos vista temporal con las tablas candidatas
df_source.createOrReplaceTempView("vw_source_tables")

spark.sql(f"""
MERGE INTO {FULL_CTL_TABLE} AS tgt
USING vw_source_tables AS src
  ON  tgt.fqn_table = src.fqn_table

-- Tablas nuevas en el origen -> se insertan con valores por defecto
WHEN NOT MATCHED BY TARGET THEN
  INSERT (
    catalog_name, schema_name, table_name, fqn_table,
    flg_vacuum, vacuum_retention_days, flg_optimize, flg_active,
    created_timestamp, updated_timestamp
  )
  VALUES (
    src.catalog_name, src.schema_name, src.table_name, src.fqn_table,
    1, 7, 1, 1,
    current_timestamp(), current_timestamp()
  )
 
-- (Opcional) Reactivar una tabla que vuelve a aparecer en el origen:
-- WHEN MATCHED AND tgt.flg_active = 0 THEN
--   UPDATE SET
--     tgt.flg_active        = 1,
--     tgt.updated_timestamp = current_timestamp()
 
-- Tablas que existen en el control pero ya no en el origen -> se desactivan
WHEN NOT MATCHED BY SOURCE AND tgt.flg_active = 1 THEN
  UPDATE SET
    tgt.flg_active        = 0,
    tgt.updated_timestamp  = current_timestamp()
""")

print("MERGE ejecutado correctamente.")

display(spark.table(FULL_CTL_TABLE).orderBy("catalog_name", "schema_name", "table_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. DataFrame de tablas activas
# MAGIC
# MAGIC Se filtra la tabla de control para quedarnos únicamente con las tablas vigentes
# MAGIC (`flg_active = 1`), que son las candidatas reales a mantenimiento.

# COMMAND ----------

df_active = spark.table(FULL_CTL_TABLE).where(F.col("flg_active") == 1)

print(f"Total de tablas activas: {df_active.count()}")
display(df_active)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Reparto equitativo en 2 grupos
# MAGIC
# MAGIC Se utiliza la función de ventana `ntile(2)` sobre un orden determinístico
# MAGIC (`catalog_name`, `schema_name`, `table_name`) para repartir los registros de
# MAGIC forma equilibrada entre 2 grupos (`1` y `2`). `ntile` garantiza que la diferencia
# MAGIC de tamaño entre grupos sea, como máximo, de 1 registro.

# COMMAND ----------

from pyspark.sql.window import Window

window_spec = Window.orderBy("catalog_name", "schema_name", "table_name")

df_active_grouped = df_active.withColumn(
    "bucket",
    F.ntile(BUCKETS).over(window_spec)
)

print("Distribución de registros por grupo:")
display(df_active_grouped.groupBy("bucket").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Resumen final

# COMMAND ----------

total_ctl     = spark.table(FULL_CTL_TABLE).count()
total_active  = df_active.count()

print(f"Tabla de control                : {FULL_CTL_TABLE}")
print(f"Total de registros en control    : {total_ctl}")
print(f"Total de tablas activas          : {total_active}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Creación de Listas

# COMMAND ----------

# Process each bucket dynamically
for i in range(1, (BUCKETS+1)):
    paths_bkt = df_active_grouped.select("fqn_table").filter(F.col("bucket") == i).collect()
    paths_bkt = [p[0] for p in paths_bkt]
    
    print(f"n_iteration: {i} - prm_paths_bkt_{i:02}: {paths_bkt}")

    dbutils.jobs.taskValues.set(key=f'prm_paths_bkt_{i:02}', value=paths_bkt)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notas de uso
# MAGIC - `df_final` queda disponible en memoria para, por ejemplo, iterar sobre cada
# MAGIC   `full_table_name` y ejecutar `OPTIMIZE`/`VACUUM` según `flg_optimize` /
# MAGIC   `flg_vacuum` y `vacuum_retention_days`.
# MAGIC - El campo `maintenance_group` permite, por ejemplo, correr el grupo `1` en un
# MAGIC   job/horario y el grupo `2` en otro, para distribuir la carga de mantenimiento.
# MAGIC - Este notebook es **idempotente**: puede reprogramarse para ejecutarse
# MAGIC   periódicamente (por ejemplo, vía un Job con trigger programado) y mantendrá
# MAGIC   la tabla de control sincronizada con el estado real del metastore.

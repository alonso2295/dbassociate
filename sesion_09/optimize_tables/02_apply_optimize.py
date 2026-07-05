# Databricks notebook source
# MAGIC %md
# MAGIC ## Importamos librerías

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from delta.tables import *
import pyspark.sql.functions as F


# COMMAND ----------

# MAGIC %md
# MAGIC ## Definimos parámetros

# COMMAND ----------

dbutils.widgets.text("fqn_table", "", "Tabla candidata a ser optimzada/purgada")
FQN_TABLE = dbutils.widgets.get("fqn_table")
print(FQN_TABLE)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Definimos variables de optimización

# COMMAND ----------

df_table_configuration = spark.sql(f"SELECT * FROM dbassociate.ctl_tb.lakehouse_tables WHERE fqn_table = '{FQN_TABLE}'")
display(df_table_configuration)

# Convertimos de DF a Row con la funcion first()
df_table_configuration = df_table_configuration.first()

# Extraemos valores
flg_vacuum = df_table_configuration["flg_vacuum"]
retention_period_days = df_table_configuration["vacuum_retention_days"]
flg_optimize = df_table_configuration["flg_optimize"]


# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicamos técnicas de optimización

# COMMAND ----------

try:
    if flg_optimize == 1:
        optimize_result = spark.sql(f"OPTIMIZE {FQN_TABLE}")
        print("Optimize Aplicado")
    if flg_vacuum == 1:
        vacuum_result = spark.sql(f"VACUUM {FQN_TABLE} RETAIN {retention_period_days * 24} HOURS")
        print("Vacuum Aplicado")
except Exception as e:
    raise(e)

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {FQN_TABLE}"))

# COMMAND ----------

dbutils.notebook.exit("Termino con éxito.")

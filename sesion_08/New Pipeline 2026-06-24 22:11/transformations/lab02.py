from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="dbassociate.bronze.cambios_clientes",
    comment="Eventos CDC de cambios en clientes (INSERT/UPDATE/DELETE).",
    table_properties={"quality": "bronze"},
)
def bronze_cambios_clientes():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.schemaLocation",
                    "/Volumes/dbassociate/default/vol_landing/sesion_08/_schemas/bronze_cambios_clientes")
            .load("/Volumes/dbassociate/default/vol_landing/sesion_08/clientes/")
            .withColumn("event_ts", F.to_timestamp("event_ts"))
            .withColumn("_ingested_at", F.current_timestamp())
    )

dp.create_streaming_table(
    name="dbassociate.silver.dim_clientes",
    comment="Dimensión de clientes con historia completa (SCD Type 2).",
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target="dbassociate.silver.dim_clientes",
    source="dbassociate.bronze.cambios_clientes",
    keys=["cliente_id"],
    sequence_by=F.col("event_ts"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["evento_id", "operation", "event_ts", "_ingested_at"],
    stored_as_scd_type=2,
)
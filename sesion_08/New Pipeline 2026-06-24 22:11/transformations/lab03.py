from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="dbassociate.bronze.cambios_productos",
    comment="Eventos CDC del catalogo de productos desde el Volume."
)
def bronze_cambios_productos():
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option(
                "cloudFiles.schemaLocation",
                "/Volumes/dbassociate/default/vol_landing/sesion_08/_schemas/bronze_productos"
            )
            .load("/Volumes/dbassociate/default/vol_landing/sesion_08/productos/")
            .withColumn("_ingested_at", F.current_timestamp())
    )

dp.create_streaming_table(
    name="dbassociate.silver.dim_productos",
    comment="Catalogo de productos — estado actual (SCD Type 1, sin historia)"
)

dp.create_auto_cdc_flow(
    target="dbassociate.silver.dim_productos",
    source="dbassociate.bronze.cambios_productos",
    keys=["producto_id"],
    sequence_by=F.col("event_ts"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["evento_id", "operation", "event_ts", "_ingested_at"],
    stored_as_scd_type=1
)
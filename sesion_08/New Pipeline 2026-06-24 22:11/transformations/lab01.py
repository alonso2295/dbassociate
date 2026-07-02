# Importaciones del pipeline (API moderna)
# IMPORTANTE: NO usar 'import dlt' (legacy). La API actual es pyspark.pipelines.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="dbassociate.bronze.ventas",
    comment="Ingesta cruda de ventas diarias desde el Volume.",
    table_properties={"quality": "bronze"},
)
def bronze_ventas():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.schemaLocation",
                    "/Volumes/dbassociate/default/vol_landing/sesion_08/_schemas/bronze_ventas")
            .load("/Volumes/dbassociate/default/vol_landing/sesion_08/ventas/")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
    )

@dp.table(
    name="dbassociate.silver.ventas",
    comment="Ventas validadas y tipadas. Filas con total <= 0 o fecha futura se descartan.",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("total_positivo", "CAST(total AS DOUBLE) > 0")
@dp.expect_or_drop("fecha_valida", "to_date(fecha) <= current_date()")
@dp.expect("region_no_nula", "region IS NOT NULL")  # warn-only
def silver_ventas():
    return (
        spark.readStream.table("dbassociate.bronze.ventas")
            .select(
                F.col("venta_id"),
                F.to_date("fecha").alias("fecha"),
                F.col("sucursal"),
                F.col("producto_id"),
                F.col("cantidad").cast("int").alias("cantidad"),
                F.col("precio_unit").cast("decimal(10,2)").alias("precio_unit"),
                F.col("total").cast("decimal(12,2)").alias("total"),
                F.col("vendedor"),
                F.col("region"),
                F.col("_ingested_at"),
            )
    )
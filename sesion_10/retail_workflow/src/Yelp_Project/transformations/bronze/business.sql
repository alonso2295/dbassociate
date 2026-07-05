CREATE OR REFRESH MATERIALIZED VIEW dbassociate.${bronze_schema}.b_business
AS
SELECT
    *
FROM read_files(
    "/Volumes/dbassociate/default/vol_landing/sesion_08/yelp_academic_dataset_business.json",
    format => "json"
)

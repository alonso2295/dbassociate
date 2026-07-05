CREATE OR REFRESH MATERIALIZED VIEW dbassociate.${bronze_schema}.b_review
AS
SELECT
    *
FROM read_files(
    "/Volumes/dbassociate/default/vol_landing/sesion_08/yelp_academic_dataset_review.json",
    format => "json"
)
;

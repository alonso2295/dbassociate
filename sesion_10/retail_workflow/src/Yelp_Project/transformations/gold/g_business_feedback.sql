CREATE OR REFRESH MATERIALIZED VIEW dbassociate.${gold_schema}.g_business_review
(
    CONSTRAINT name_not_null EXPECT (name IS NOT NULL),
    CONSTRAINT noiselevel_not_null EXPECT (noiselevel IS NOT NULL)
)
AS
SELECT
    b.business_id,
    b.name,
    b.address,
    b.wifi,
    b.noiselevel,
    b.dogsallowed,
    b.open24hours,
    b.stars,
    b.is_open,
    r.review_id,
    r.date,
    r.text,
    r.review_sentiment
FROM dbassociate.${silver_schema}.s_business b
INNER JOIN  dbassociate.${silver_schema}.s_review r
    ON b.business_id = r.business_id
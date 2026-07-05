CREATE OR REFRESH MATERIALIZED VIEW dbassociate.${silver_schema}.s_review
AS
SELECT
    business_id,
    review_id,
    user_id,
    date,
    text,
    stars,
    case 
        when stars == 1 then 'very negative'
        when stars == 2 then 'negative'
        when stars == 3 then 'neutral'
        when stars == 4 then 'positive'
        when stars == 5 then 'very positive'
    end as review_sentiment    
FROM dbassociate.${bronze_schema}.b_review
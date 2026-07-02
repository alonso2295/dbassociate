CREATE OR REFRESH MATERIALIZED VIEW dbassociate.silver.s_business
AS
SELECT
    business_id,
    name,
    address,
    city,
    hours,
    attributes.WiFi as wifi,
    attributes.NoiseLevel as noiselevel,
    case 
        when attributes.DogsAllowed is null then 'False'
        else attributes.DogsAllowed
    end as dogsallowed,
    attributes.Open24Hours as open24hours,
    is_open,
    review_count,
    stars
from dbassociate.bronze.b_business


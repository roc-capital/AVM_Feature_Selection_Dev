WITH census_data AS (
    SELECT
        LEFT(a.CENSUS_BLOCK_GROUP, 11) AS census_tract,

        -- Education
        SUM(a."B15002e1")  AS total_population_25plus,
        SUM(a."B15002e15") AS male_bachelors_degree,
        SUM(a."B15002e32") AS female_bachelors_degree,
        CASE
            WHEN SUM(a."B15002e1") > 0
            THEN 100.0 * (SUM(a."B15002e15") + SUM(a."B15002e32"))
                 / SUM(a."B15002e1")
        END AS pct_bachelors_degree,

        -- Earnings (averages of BG medians)
        AVG(d."B20002e1") AS median_earnings_total,
        AVG(d."B20002e2") AS median_earnings_male,
        AVG(d."B20002e3") AS median_earnings_female,

        -- Household Income
        AVG(income."B19013e1") AS median_household_income,

        -- Housing characteristics
        AVG(b25."B25077e1") AS median_home_value,
        AVG(b25."B25064e1") AS median_gross_rent,

        SUM(b25."B25003e2") AS owner_occupied_units,
        SUM(b25."B25003e3") AS renter_occupied_units,
        CASE
            WHEN SUM(b25."B25003e1") > 0
            THEN 100.0 * SUM(b25."B25003e2")
                 / SUM(b25."B25003e1")
        END AS pct_owner_occupied,

        SUM(b25."B25002e2") AS occupied_units,
        SUM(b25."B25002e3") AS vacant_units,

        -- Demographics
        AVG(age."B01002e1") AS median_age,

        -- Employment
        SUM(employment."B23025e3") AS civilian_employed,
        SUM(employment."B23025e5") AS civilian_unemployed,
        CASE
            WHEN (SUM(employment."B23025e3") + SUM(employment."B23025e5")) > 0
            THEN 100.0 * SUM(employment."B23025e5")
                 / (SUM(employment."B23025e3") + SUM(employment."B23025e5"))
        END AS unemployment_rate,

        -- Poverty (temporarily disabled: column name mismatch in 2019_CBG_B17)
        CAST(NULL AS NUMBER) AS population_below_poverty,
        CAST(NULL AS FLOAT)  AS poverty_rate

    FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_B15" a
    LEFT JOIN US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_B03" c
        ON a.CENSUS_BLOCK_GROUP = c.CENSUS_BLOCK_GROUP
    LEFT JOIN US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_B20" d
        ON a.CENSUS_BLOCK_GROUP = d.CENSUS_BLOCK_GROUP
    LEFT JOIN US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_B19" income
        ON a.CENSUS_BLOCK_GROUP = income.CENSUS_BLOCK_GROUP
    LEFT JOIN US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_B25" b25
        ON a.CENSUS_BLOCK_GROUP = b25.CENSUS_BLOCK_GROUP
    LEFT JOIN US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_B01" age
        ON a.CENSUS_BLOCK_GROUP = age.CENSUS_BLOCK_GROUP
    LEFT JOIN US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_B23" employment
        ON a.CENSUS_BLOCK_GROUP = employment.CENSUS_BLOCK_GROUP

    -- Keep the join if you want to validate it exists; remove it if unnecessary
    LEFT JOIN US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_B17" poverty
        ON a.CENSUS_BLOCK_GROUP = poverty.CENSUS_BLOCK_GROUP

    WHERE LEFT(a.CENSUS_BLOCK_GROUP, 2) = '37'  -- NC state FIPS
    GROUP BY 1
)

SELECT
    p.PROPERTYID AS property_id,
    p.CURRENTSALESPRICE AS sale_price,
    p.CURRENTSALERECORDINGDATE AS sale_date,
    p.YEARBUILT AS year_built,
    p.EFFECTIVEYEARBUILT AS effective_year_built,
    p.SITUSLATITUDE AS latitude,
    p.SITUSLONGITUDE AS longitude,
    p.SITUSSTATE AS state,
    p.SITUSCITY AS city,
    p.SITUSZIP5 AS zip,
    p.SITUSCENSUSTRACT AS census_tract,
    p.SUMLIVINGAREASQFT AS living_sqft,
    p.LOTSIZESQFT AS lot_sqft,
    p.BEDROOMS AS bedrooms,
    p.BATHFULL AS full_baths,
    p.BATHSPARTIALNBR AS half_baths,
    p.GARAGEPARKINGNBR AS garage_spaces,
    p.FIREPLACECODE AS fireplace_code,

    -- Census: Education
    c.total_population_25plus,
    c.male_bachelors_degree,
    c.female_bachelors_degree,
    c.pct_bachelors_degree,

    -- Census: Population
    c.total_population,
    c.non_hispanic_white_population,
    c.pct_white,

    -- Census: Income
    c.median_earnings_total,
    c.median_earnings_male,
    c.median_earnings_female,
    c.median_household_income,

    -- Census: Housing
    c.median_home_value,
    c.median_gross_rent,
    c.owner_occupied_units,
    c.renter_occupied_units,
    c.pct_owner_occupied,
    c.occupied_units,
    c.vacant_units,

    -- Census: Age & Employment
    c.median_age,
    c.civilian_employed,
    c.civilian_unemployed,
    c.unemployment_rate,

    -- Census: Poverty
    c.population_below_poverty,
    c.poverty_rate,

    -- Election data
    v.county_name,
    v.county_fips,
    v.votes_gop,
    v.votes_dem,
    v.total_votes,
    v.per_gop,
    v.per_dem,
    v.per_point_diff,
    v.dem_margin,
    v.rep_margin

FROM roc_public_record_data."DATATREE"."ASSESSOR" p
LEFT JOIN census_data c
    ON p.SITUSCENSUSTRACT = c.census_tract
LEFT JOIN "SCRATCH"."DATASCIENCE"."VOTING_PATTERNS_2020" v
    ON LEFT(p.FIPS, 5) = CAST(v.county_fips AS VARCHAR)

WHERE
    LOWER(p.SITUSSTATE) = 'nc'
    AND p.CURRENTSALESPRICE IS NOT NULL
    AND p.SUMLIVINGAREASQFT IS NOT NULL
    AND p.LOTSIZESQFT IS NOT NULL
    AND p.SITUSLATITUDE IS NOT NULL
    AND p.SITUSLONGITUDE IS NOT NULL;

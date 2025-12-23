WITH census_data AS (
    SELECT
        LEFT(a.CENSUS_BLOCK_GROUP, 11) AS census_tract,

        -- Education
        SUM(a."B15002e1")  AS total_population_25plus,
        SUM(a."B15002e15") AS male_bachelors_degree,
        SUM(a."B15002e32") AS female_bachelors_degree,
        CASE
            WHEN SUM(a."B15002e1") > 0
            THEN 100.0 * (SUM(a."B15002e15") + SUM(a."B15002e32")) / SUM(a."B15002e1")
        END AS pct_bachelors_degree,

        -- Earnings
        AVG(d."B20002e1") AS median_earnings_total,
        AVG(d."B20002e2") AS median_earnings_male,
        AVG(d."B20002e3") AS median_earnings_female,

        -- Household Income
        AVG(income."B19013e1") AS median_household_income,

        -- Housing
        AVG(b25."B25077e1") AS median_home_value,
        AVG(b25."B25064e1") AS median_gross_rent,
        SUM(b25."B25003e2") AS owner_occupied_units,
        SUM(b25."B25003e3") AS renter_occupied_units,
        CASE
            WHEN SUM(b25."B25003e1") > 0
            THEN 100.0 * SUM(b25."B25003e2") / SUM(b25."B25003e1")
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

        -- Poverty (disabled / placeholder)
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
    LEFT JOIN US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_B17" poverty
        ON a.CENSUS_BLOCK_GROUP = poverty.CENSUS_BLOCK_GROUP

    WHERE LEFT(a.CENSUS_BLOCK_GROUP, 2) = '37'   -- NC only
    GROUP BY 1
)
SELECT
    p.*,
    c.*,
    v.*
FROM roc_public_record_data."DATATREE"."ASSESSOR" p
LEFT JOIN census_data c
    ON p.SITUSCENSUSTRACT = c.census_tract
LEFT JOIN "SCRATCH"."DATASCIENCE"."VOTING_PATTERNS_2020" v
    ON LEFT(p.FIPS, 5) = CAST(v.county_fips AS VARCHAR);

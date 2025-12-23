WITH census_data AS (
    SELECT
        LEFT(a.CENSUS_BLOCK_GROUP, 11) as census_tract,

        -- Education
        SUM(a."B15002e1") AS total_population_25plus,
        SUM(a."B15002e15") AS male_bachelors_degree,
        SUM(a."B15002e32") AS female_bachelors_degree,
        CASE WHEN SUM(a."B15002e1") > 0
             THEN (SUM(a."B15002e15" + a."B15002e32") / SUM(a."B15002e1")) * 100
             ELSE NULL END AS pct_bachelors_degree,

        -- Earnings
        AVG(d."B20002e1") AS median_earnings_total,
        AVG(d."B20002e2") AS median_earnings_male,
        AVG(d."B20002e3") AS median_earnings_female,

        -- Household Income
        AVG(income."B19013e1") AS median_household_income,

        -- Housing characteristics
        AVG(housing_value."B25077e1") AS median_home_value,
        AVG(rent."B25064e1") AS median_gross_rent,
        SUM(tenure."B25003e2") AS owner_occupied_units,
        SUM(tenure."B25003e3") AS renter_occupied_units,
        CASE WHEN SUM(tenure."B25003e1") > 0
             THEN (SUM(tenure."B25003e2") / SUM(tenure."B25003e1")) * 100
             ELSE NULL END AS pct_owner_occupied,
        SUM(occupancy."B25002e2") AS occupied_units,
        SUM(occupancy."B25002e3") AS vacant_units,

        -- Demographics
        AVG(age."B01002e1") AS median_age,

        -- Employment
        SUM(employment."B23025e3") AS civilian_employed,
        SUM(employment."B23025e5") AS civilian_unemployed,
        CASE WHEN SUM(employment."B23025e3" + employment."B23025e5") > 0
             THEN (SUM(employment."B23025e5") / SUM(employment."B23025e3" + employment."B23025e5")) * 100
             ELSE NULL END AS unemployment_rate

    FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_B15" AS a
    LEFT JOIN US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_B03" AS c
        ON a.CENSUS_BLOCK_GROUP = c.CENSUS_BLOCK_GROUP
    LEFT JOIN US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_B20" AS d
        ON a.CENSUS_BLOCK_GROUP = d.CENSUS_BLOCK_GROUP
    LEFT JOIN US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_B19" AS income
        ON a.CENSUS_BLOCK_GROUP = income.CENSUS_BLOCK_GROUP
    LEFT JOIN US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_B25" AS housing_value
        ON a.CENSUS_BLOCK_GROUP = housing_value.CENSUS_BLOCK_GROUP
    LEFT JOIN US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_B25" AS rent
        ON a.CENSUS_BLOCK_GROUP = rent.CENSUS_BLOCK_GROUP
    LEFT JOIN US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_B25" AS tenure
        ON a.CENSUS_BLOCK_GROUP = tenure.CENSUS_BLOCK_GROUP
    LEFT JOIN US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_B25" AS occupancy
        ON a.CENSUS_BLOCK_GROUP = occupancy.CENSUS_BLOCK_GROUP
    LEFT JOIN US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_B01" AS age
        ON a.CENSUS_BLOCK_GROUP = age.CENSUS_BLOCK_GROUP
    LEFT JOIN US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_B23" AS employment
        ON a.CENSUS_BLOCK_GROUP = employment.CENSUS_BLOCK_GROUP

    WHERE LEFT(a.CENSUS_BLOCK_GROUP, 2) IN ('37', '36', '39')  -- NC, NY, OH state FIPS codes
    GROUP BY LEFT(a.CENSUS_BLOCK_GROUP, 11)
)

SELECT
    p.PROPERTYID as property_id,
    p.CURRENTSALESPRICE as sale_price,
    p.CURRENTSALERECORDINGDATE as sale_date,
    p.YEARBUILT as year_built,
    p.EFFECTIVEYEARBUILT as effective_year_built,
    p.SITUSLATITUDE as latitude,
    p.SITUSLONGITUDE as longitude,
    p.SITUSSTATE as state,
    p.SITUSCITY as city,
    p.SITUSZIP5 as zip,
    p.SITUSCENSUSTRACT as census_tract_original,
    c.census_tract as census_tract_matched,
    p.SUMLIVINGAREASQFT as living_sqft,
    p.LOTSIZESQFT as lot_sqft,
    p.BEDROOMS as bedrooms,
    p.BATHFULL as full_baths,
    p.BATHSPARTIALNBR as half_baths,
    p.GARAGEPARKINGNBR as garage_spaces,
    p.FIREPLACECODE as fireplace_code,

    -- Census Demographics (Education)
    c.total_population_25plus,
    c.male_bachelors_degree,
    c.female_bachelors_degree,
    c.pct_bachelors_degree,

    -- Census Demographics (Population)
    c.total_population,
    c.non_hispanic_white_population,
    c.pct_white,

    -- Census Demographics (Income)
    c.median_earnings_total,
    c.median_earnings_male,
    c.median_earnings_female,
    c.median_household_income,

    -- Census Demographics (Housing)
    c.median_home_value,
    c.median_gross_rent,
    c.owner_occupied_units,
    c.renter_occupied_units,
    c.pct_owner_occupied,
    c.occupied_units,
    c.vacant_units,

    -- Census Demographics (Age & Employment)
    c.median_age,
    c.civilian_employed,
    c.civilian_unemployed,
    c.unemployment_rate,

    -- Election Data
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

FROM
    roc_public_record_data."DATATREE"."ASSESSOR" p

LEFT JOIN
    census_data c
    ON LEFT(p.FIPS, 5) || p.SITUSCENSUSTRACT = c.census_tract

LEFT JOIN
    "SCRATCH"."DATASCIENCE"."VOTING_PATTERNS_2020" v
    ON LEFT(p.FIPS, 5) = CAST(v.county_fips AS VARCHAR)

WHERE
    (
        -- NC around Raleigh (Wake County and nearby counties in Triangle area)
        (p.SITUSSTATE = 'nc' AND LEFT(p.FIPS, 5) IN ('37183', '37063', '37101', '37069', '37135'))  -- Wake, Durham, Johnston, Franklin, Orange
        OR
        -- New York excluding NYC (Bronx, Kings, New York, Queens, Richmond)
        (p.SITUSSTATE = 'ny' AND LEFT(p.FIPS, 5) NOT IN ('36005', '36047', '36061', '36081', '36085'))
        OR
        -- All of Ohio
        (p.SITUSSTATE = 'oh')
    )
    AND p.CURRENTSALESPRICE IS NOT NULL
    AND p.SUMLIVINGAREASQFT IS NOT NULL
    AND p.LOTSIZESQFT IS NOT NULL
    AND p.SITUSLATITUDE IS NOT NULL
    AND p.SITUSLONGITUDE IS NOT NULL;
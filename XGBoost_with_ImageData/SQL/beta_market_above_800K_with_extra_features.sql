WITH census_data AS (
    SELECT
        LEFT(a.CENSUS_BLOCK_GROUP, 11) AS census_tract,

        -- Education
        SUM(a."B15002e1")  AS total_population_25plus,
        SUM(a."B15002e15") AS male_bachelors_degree,
        SUM(a."B15002e32") AS female_bachelors_degree,
        CASE WHEN SUM(a."B15002e1") > 0
          THEN 100.0 * (SUM(a."B15002e15") + SUM(a."B15002e32")) / SUM(a."B15002e1")
        END AS pct_bachelors_degree,

        -- Population & Demographics
        SUM(c."B03002e1") AS total_population,
        SUM(c."B03002e3") AS non_hispanic_white_population,
        CASE WHEN SUM(c."B03002e1") > 0
          THEN 100.0 * SUM(c."B03002e3") / SUM(c."B03002e1")
        END AS pct_white,

        -- Income & Earnings
        AVG(d."B20002e1") AS median_earnings_total,
        AVG(d."B20002e2") AS median_earnings_male,
        AVG(d."B20002e3") AS median_earnings_female,
        AVG(income."B19013e1") AS median_household_income,

        -- Housing
        AVG(b25."B25077e1") AS median_home_value,
        AVG(b25."B25064e1") AS median_gross_rent,
        SUM(b25."B25003e2") AS owner_occupied_units,
        SUM(b25."B25003e3") AS renter_occupied_units,
        SUM(b25."B25002e2") AS occupied_units,
        SUM(b25."B25002e3") AS vacant_units,
        CASE WHEN SUM(b25."B25003e1") > 0
          THEN 100.0 * SUM(b25."B25003e2") / SUM(b25."B25003e1")
        END AS pct_owner_occupied,

        -- Derived: Vacancy Rate
        CASE WHEN (SUM(b25."B25002e2") + SUM(b25."B25002e3")) > 0
          THEN 100.0 * SUM(b25."B25002e3") / (SUM(b25."B25002e2") + SUM(b25."B25002e3"))
        END AS vacancy_rate,

        -- Age & Employment
        AVG(age."B01002e1") AS median_age,
        SUM(employment."B23025e3") AS civilian_employed,
        SUM(employment."B23025e5") AS civilian_unemployed,
        CASE WHEN (SUM(employment."B23025e3") + SUM(employment."B23025e5")) > 0
          THEN 100.0 * SUM(employment."B23025e5")
               / (SUM(employment."B23025e3") + SUM(employment."B23025e5"))
        END AS unemployment_rate,

        -- Poverty (currently NULL but structure in place)
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
    GROUP BY 1
),
base AS (
    SELECT
        -- Property Identifiers
        p.PROPERTYID as property_id,
        p.CURRENTSALESPRICE as sale_price,
        p.CURRENTSALERECORDINGDATE as sale_date,

        -- Location
        p.SITUSLATITUDE as latitude,
        p.SITUSLONGITUDE as longitude,
        p.SITUSSTATE as state,
        p.SITUSCITY as city,
        p.SITUSZIP5 as zip,
        p.SITUSCENSUSTRACT as census_tract,

        -- Property Age (USE EFFECTIVE YEAR BUILT!)
        p.YEARBUILT as year_built,
        p.EFFECTIVEYEARBUILT as effective_year_built,
        COALESCE(p.EFFECTIVEYEARBUILT, p.YEARBUILT) as year_built_final,

        -- Property Size
        p.SUMLIVINGAREASQFT as living_sqft,
        p.LOTSIZESQFT as lot_sqft,
        p.LOTSIZEACRES as lot_acres,
        p.BEDROOMS as bedrooms,
        p.BATHFULL as full_baths,
        p.BATHSPARTIALNBR as half_baths,
        p.TOTALROOMS as total_rooms,

        -- Property Features
        p.GARAGEPARKINGNBR as garage_spaces,
        p.FIREPLACECODE as fireplace_code,
        CASE WHEN p.FIREPLACECODE IS NOT NULL THEN 1 ELSE 0 END as has_fireplace,

        -- ========================================================================
        -- CRITICAL LUXURY FEATURES FOR ULTRA-HIGH PROPERTIES
        -- ========================================================================

        -- Building Quality & Condition (HIGHEST PRIORITY FOR ULTRA-HIGH)
        p.BUILDINGQUALITYCODE as building_quality,
        p.BUILDINGCONDITIONCODE as building_condition,
        p.STYLECODE as architectural_style,

        -- Luxury Amenities
        p.POOLCODE as pool_code,
        CASE WHEN p.POOLCODE IS NOT NULL AND p.POOLCODE > 0 THEN 1 ELSE 0 END as has_pool,
        p.STORIESNBRCODE as stories,

        -- Climate Control
        p.AIRCONDITIONINGCODE as ac_code,
        CASE WHEN p.AIRCONDITIONINGCODE IS NOT NULL AND p.AIRCONDITIONINGCODE > 0 THEN 1 ELSE 0 END as has_ac,

        -- Basement
        p.BASEMENTCODE as basement_code,
        p.BASEMENTFINISHEDSQFT as basement_finished_sqft,
        CASE WHEN p.BASEMENTCODE IS NOT NULL THEN 1 ELSE 0 END as has_basement,

        -- Water Features
        p.WATERCODE as water_code,
        CASE WHEN p.WATERCODE IS NOT NULL AND p.WATERCODE > 0 THEN 1 ELSE 0 END as has_water_feature,

        -- ========================================================================
        -- Assessment & Market Values (predictive but lagged)
        -- ========================================================================
        p.ASSDTOTALVALUE as assessed_total_value,
        p.ASSDLANDVALUE as assessed_land_value,
        p.ASSDIMPROVEMENTVALUE as assessed_improvement_value,
        p.MARKETVALUELAND as market_value_land,
        p.MARKETVALUEIMPROVEMENT as market_value_improvements,

        -- Site Characteristics
        p.TOPOGRAPHYCODE as topography_code,
        p.SITEINFLUENCECODE as site_influence_code,

        -- Building Materials
        p.EXTERIORWALLSCODE as exterior_walls_code,
        p.ROOFCOVERCODE as roof_cover_code,

        -- Ownership
        p.OWNEROCCUPIED as is_owner_occupied,

        -- Transaction History
        p.PREVSALESPRICE as previous_sale_price,
        p.PREVSALERECORDINGDATE as previous_sale_date,

        -- Community
        p.SUBDIVISIONNAME as subdivision,
        p.ZONING as zoning,

        -- ========================================================================

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
        c.vacancy_rate,

        -- Census Demographics (Age & Employment)
        c.median_age,
        c.civilian_employed,
        c.civilian_unemployed,
        c.unemployment_rate,

        -- Derived: Wealth Concentration Metrics
        c.median_household_income * c.total_population / 1000000.0 as wealth_concentration_index,
        c.pct_bachelors_degree * c.median_household_income / 100000.0 as education_income_index,

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
        v.rep_margin,

        -- Derived: Political Lean Strength (absolute difference)
        ABS(v.per_gop - v.per_dem) as political_lean_strength,

        -- State FIPS for partitioning
        LEFT(LPAD(TRIM(CAST(p.FIPS AS VARCHAR)), 5, '0'), 2) AS state_fips

    FROM roc_public_record_data."DATATREE"."ASSESSOR" p
    LEFT JOIN census_data c
        ON p.SITUSCENSUSTRACT = c.census_tract
    LEFT JOIN "SCRATCH"."DATASCIENCE"."VOTING_PATTERNS_2020" v
        ON LEFT(LPAD(TRIM(CAST(p.FIPS AS VARCHAR)), 5, '0'), 5) = LPAD(TRIM(CAST(v.county_fips AS VARCHAR)), 5, '0')
    WHERE
        -- Top 10 states by population/property value
        p.SITUSSTATE IN ('ca','tx','ny','fl','il','pa','oh','ga','wa','nj')

        -- Data quality filters
        AND p.CURRENTSALESPRICE IS NOT NULL
        AND p.SUMLIVINGAREASQFT IS NOT NULL
        AND p.LOTSIZESQFT IS NOT NULL
        AND p.SITUSLATITUDE IS NOT NULL
        AND p.SITUSLONGITUDE IS NOT NULL

        -- Exclude obvious data errors
        AND p.CURRENTSALESPRICE BETWEEN 10000 AND 100000000
        AND p.SUMLIVINGAREASQFT > 0
        AND p.LOTSIZESQFT > 0
        AND p.BEDROOMS > 0
),
random_sample AS (
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY state_fips
        ORDER BY RANDOM()
    ) <= 1000
),
high_value_properties AS (
    SELECT *
    FROM base
    WHERE sale_price > 800000
)
-- Combine random sample with all high-value properties, removing duplicates
SELECT *
FROM random_sample
UNION
SELECT *
FROM high_value_properties
ORDER BY sale_price DESC;


--==========================================================================--
--            StoredProc for Ingesting data into dimension tables
--==========================================================================--


CREATE OR REPLACE PROCEDURE REALESTATE.GOLD.populate_real_estate_dimensions()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

  -- 1. POPULATE DATE DIMENSION
  INSERT INTO REALESTATE.GOLD.dim_date
  WITH date_spine AS (
    SELECT DATEADD('day', seq4(), '2020-01-01') AS full_date
    FROM TABLE(GENERATOR(ROWCOUNT => 3653))
  )
  SELECT 
      TO_NUMBER(TO_CHAR(full_date, 'YYYYMMDD')) AS date_key,
      full_date,
      YEAR(full_date),
      QUARTER(full_date),
      MONTH(full_date),
      MONTHNAME(full_date),
      WEEK(full_date),
      DAYOFMONTH(full_date),
      DAYOFWEEK(full_date),
      DAYNAME(full_date),
      CASE WHEN DAYOFWEEK(full_date) IN (1, 7) THEN TRUE ELSE FALSE END,
      FALSE,
      CASE 
          WHEN MONTH(full_date) >= 4 THEN YEAR(full_date)
          ELSE YEAR(full_date) - 1 
      END,
      CASE 
          WHEN MONTH(full_date) IN (4,5,6) THEN 1
          WHEN MONTH(full_date) IN (7,8,9) THEN 2
          WHEN MONTH(full_date) IN (10,11,12) THEN 3
          ELSE 4
      END
  FROM date_spine;

  -- 2. POPULATE CITY DIMENSION
  INSERT INTO REALESTATE.GOLD.dim_city (city_name, state_name, region, country)
  SELECT DISTINCT 
      COALESCE(city, 'Unknown'),
      COALESCE(state, 'Unknown'),
      COALESCE(region, 'Unknown'),
      'India'
  FROM REALESTATE.SILVER.properties
  WHERE city IS NOT NULL;

  -- 3. POPULATE LOCALITY DIMENSION
  INSERT INTO REALESTATE.GOLD.dim_locality (locality_name, city_key, latitude, longitude, area_type, tier)
  SELECT DISTINCT 
      COALESCE(p.locality, 'Unknown'),
      c.city_key,
      p.latitude,
      p.longitude,
      'Residential',
      CASE 
          WHEN p.max_price > 10000000 THEN 'Tier 1'
          WHEN p.max_price > 5000000 THEN 'Tier 2'
          ELSE 'Tier 3'
      END
  FROM REALESTATE.SILVER.properties p
  JOIN REALESTATE.GOLD.dim_city c ON COALESCE(p.city, 'Unknown') = c.city_name
  WHERE p.locality IS NOT NULL;

  -- 4. POPULATE PROPERTY TYPE DIMENSION
  INSERT INTO REALESTATE.GOLD.dim_property_type (property_type, property_category, is_luxury)
  VALUES 
      ('Apartment', 'Residential', FALSE),
      ('Villa', 'Residential', TRUE),
      ('Independent House', 'Residential', FALSE),
      ('Plot', 'Residential', FALSE),
      ('Office Space', 'Commercial', FALSE),
      ('Shop', 'Commercial', FALSE),
      ('Warehouse', 'Industrial', FALSE),
      ('Luxury Apartment', 'Residential', TRUE),
      ('Penthouse', 'Residential', TRUE),
      ('Studio Apartment', 'Residential', FALSE);

  -- 5. POPULATE PROPERTY STATUS DIMENSION
  INSERT INTO REALESTATE.GOLD.dim_property_status (
      status_name, is_rera_approved, is_new_booking, is_under_construction, is_ready_to_move, has_project_tag
  )
  SELECT DISTINCT
      CASE
          WHEN ps.is_under_construction THEN 'Under Construction'
          WHEN ps.is_ready_to_move THEN 'Ready to Move'
          ELSE 'Unknown'
      END,
      COALESCE(ps.is_rera_approved, FALSE),
      COALESCE(ps.is_new_booking, FALSE),
      COALESCE(ps.is_under_construction, FALSE),
      COALESCE(ps.is_ready_to_move, FALSE),
      COALESCE(ps.has_project_tag, FALSE)
  FROM REALESTATE.SILVER.property_status ps;

  -- 6. POPULATE BUILDER DIMENSION
  INSERT INTO REALESTATE.GOLD.dim_builder (builder_name, builder_type, is_verified, phone, is_paid_seller, reputation_score)
  SELECT DISTINCT
      COALESCE(seller_name, 'Unknown'),
      COALESCE(seller_type, 'Unknown'),
      FALSE,
      seller_phone,
      COALESCE(seller_is_paid, FALSE),
      3
  FROM REALESTATE.SILVER.properties
  WHERE seller_name IS NOT NULL;

  -- 7. POPULATE CONFIGURATION DIMENSION
  INSERT INTO REALESTATE.GOLD.dim_configuration (
      config_type,
      config_category
  )
  SELECT DISTINCT
      TRIM(pc.config_type),
      INITCAP(TRIM(pc.config_category))
  FROM REALESTATE.SILVER.property_configurations pc
  WHERE pc.config_type IS NOT NULL
    AND pc.config_category IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM REALESTATE.GOLD.dim_configuration dim
        WHERE dim.config_type = TRIM(pc.config_type)
          AND dim.config_category = INITCAP(TRIM(pc.config_category))
    );

  -- 8. POPULATE AMENITIES DIMENSION
  INSERT INTO REALESTATE.GOLD.dim_amenities (
      has_gated_community, has_gym, has_lift, has_parking, has_pool, amenity_score
  )
  SELECT DISTINCT
      COALESCE(has_gated_community, FALSE),
      COALESCE(has_gym, FALSE),
      COALESCE(has_lift, FALSE),
      COALESCE(has_parking, FALSE),
      COALESCE(has_pool, FALSE),
      (COALESCE(has_gated_community::INT, 0) + 
       COALESCE(has_gym::INT, 0) + 
       COALESCE(has_lift::INT, 0) + 
       COALESCE(has_parking::INT, 0) + 
       COALESCE(has_pool::INT, 0))
  FROM REALESTATE.SILVER.property_amenities;

  -- Default record
  INSERT INTO REALESTATE.GOLD.dim_amenities (
      has_gated_community, has_gym, has_lift, has_parking, has_pool, amenity_score
  )
  VALUES (FALSE, FALSE, FALSE, FALSE, FALSE, 0);
  RETURN 'All dimension tables populated successfully.';

END;
$$;

CALL REALESTATE.GOLD.populate_real_estate_dimensions();



--==========================================================================--
--            StoredProc for Ingesting data into Main Fact table
--==========================================================================--


-- I have written this in java script because we can debug properly by implementing try catch block.

CREATE OR REPLACE PROCEDURE REALESTATE.GOLD.populate_fact_property_listings()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
try {

  var sql_command = `
    INSERT INTO REALESTATE.GOLD.fact_property_listings (
        date_key, locality_key, property_type_key, status_key, builder_key, 
        config_key, amenity_key, property_id, listing_url, image_url, property_name, currency,
        is_active_property, is_most_contacted, min_price, max_price, avg_price,
        price_per_sqft, property_area_sqft, min_size, max_size,
        posted_date_key, possession_date_key, days_on_market
    )
    SELECT 
        COALESCE(TO_NUMBER(TO_CHAR(p.posted_date, 'YYYYMMDD')), TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD'))),
        COALESCE(l.locality_key, 999),
        COALESCE(pt.property_type_key, 999),
        COALESCE(ps.status_key, 999),
        COALESCE(b.builder_key, 999),
        COALESCE(cfg.config_key, 999),
        COALESCE(a.amenity_key, 999),
        p.id,
        p.url,
        p.image_url,
        p.name,
        COALESCE(p.currency, 'INR'),
        COALESCE(p.is_active_property, TRUE),
        COALESCE(p.is_most_contacted, FALSE),
        p.min_price,
        p.max_price,
        COALESCE(p.property_price, (p.min_price + p.max_price)/2),
        p.avg_price_per_sqft,
        p.property_area_sqft,
        p.min_size,
        p.max_size,
        TO_NUMBER(TO_CHAR(p.posted_date, 'YYYYMMDD')),
        TO_NUMBER(TO_CHAR(p.possession_date, 'YYYYMMDD')),
        CASE 
            WHEN p.posted_date IS NOT NULL THEN DATEDIFF('day', p.posted_date::DATE, CURRENT_DATE())
            ELSE 0 
        END
    FROM REALESTATE.SILVER.properties p
    LEFT JOIN REALESTATE.SILVER.property_configurations pc ON p.id = pc.id
    LEFT JOIN REALESTATE.SILVER.property_status pst ON p.id = pst.id
    LEFT JOIN REALESTATE.SILVER.property_amenities pa ON p.id = pa.id
    LEFT JOIN (
        SELECT locality_key, locality_name, city_key,
               ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(locality_name)) ORDER BY locality_key) as rn
        FROM REALESTATE.GOLD.dim_locality
    ) l ON UPPER(TRIM(COALESCE(p.locality, 'Unknown'))) = UPPER(TRIM(l.locality_name)) AND l.rn = 1
    LEFT JOIN (
        SELECT city_key, city_name,
               ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(city_name)) ORDER BY city_key) as rn
        FROM REALESTATE.GOLD.dim_city
    ) c ON UPPER(TRIM(COALESCE(p.city, 'Unknown'))) = UPPER(TRIM(c.city_name)) AND c.rn = 1 AND l.city_key = c.city_key
    LEFT JOIN (
        SELECT property_type_key, property_type,
               ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(property_type)) ORDER BY property_type_key) as rn
        FROM REALESTATE.GOLD.dim_property_type
    ) pt ON UPPER(TRIM(
            CASE 
                WHEN LOWER(COALESCE(pc.config_category, '')) IN ('duplex', 'triplex', 'builder floor') THEN 'independent house'
                WHEN LOWER(COALESCE(pc.config_category, '')) = 'villament' THEN 'luxury apartment'
                ELSE COALESCE(pc.config_category, 'Unknown')
            END
        )) = UPPER(TRIM(pt.property_type)) AND pt.rn = 1
    LEFT JOIN (
        SELECT status_key, is_under_construction, is_ready_to_move, is_new_booking, is_rera_approved, has_project_tag,
               ROW_NUMBER() OVER (PARTITION BY is_under_construction, is_ready_to_move, is_new_booking, is_rera_approved, has_project_tag ORDER BY status_key) as rn
        FROM REALESTATE.GOLD.dim_property_status
    ) ps ON COALESCE(pst.is_under_construction, FALSE) = ps.is_under_construction
        AND COALESCE(pst.is_ready_to_move, FALSE) = ps.is_ready_to_move
        AND COALESCE(pst.is_new_booking, FALSE) = ps.is_new_booking
        AND COALESCE(pst.is_rera_approved, FALSE) = ps.is_rera_approved
        AND COALESCE(pst.has_project_tag, FALSE) = ps.has_project_tag
        AND ps.rn = 1
    LEFT JOIN (
        SELECT builder_key, builder_name,
               ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(builder_name)) ORDER BY builder_key) as rn
        FROM REALESTATE.GOLD.dim_builder
    ) b ON UPPER(TRIM(COALESCE(p.seller_name, 'Unknown'))) = UPPER(TRIM(b.builder_name)) AND b.rn = 1
    LEFT JOIN (
        SELECT config_key, config_type,
               ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(config_type)) ORDER BY config_key) as rn
        FROM REALESTATE.GOLD.dim_configuration
    ) cfg ON UPPER(TRIM(
            CASE 
                WHEN REGEXP_LIKE(COALESCE(pc.config_type, ''), '^[0-9]+(\\.[0-9]+)?\\s*[-_]*bhk', 'i') 
                    THEN REGEXP_REPLACE(LOWER(pc.config_type), '[^0-9\\.]+', '') || ' BHK'
                WHEN LOWER(COALESCE(pc.config_type, '')) LIKE '%studio%' THEN '1 BHK'
                ELSE COALESCE(pc.config_type, 'Unknown')
            END
        )) = UPPER(TRIM(cfg.config_type)) AND cfg.rn = 1
    LEFT JOIN (
        SELECT amenity_key, has_gated_community, has_gym, has_lift, has_parking, has_pool,
               ROW_NUMBER() OVER (PARTITION BY has_gated_community, has_gym, has_lift, has_parking, has_pool ORDER BY amenity_key) as rn
        FROM REALESTATE.GOLD.dim_amenities
    ) a ON COALESCE(pa.has_gated_community, FALSE) = a.has_gated_community
       AND COALESCE(pa.has_gym, FALSE) = a.has_gym
       AND COALESCE(pa.has_lift, FALSE) = a.has_lift
       AND COALESCE(pa.has_parking, FALSE) = a.has_parking
       AND COALESCE(pa.has_pool, FALSE) = a.has_pool
       AND a.rn = 1;
  `;

  snowflake.execute({ sqlText: sql_command });
  return 'Fact table "fact_property_listings" populated successfully.';

} catch (err) {
  return 'Error: ' + err.message;
}
$$;

CALL REALESTATE.GOLD.populate_fact_property_listings();


--==========================================================================--
--            StoredProc for Ingesting data into Nearby Places table
--==========================================================================--


CREATE OR REPLACE PROCEDURE REALESTATE.GOLD.populate_fact_property_nearby_places()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

  INSERT INTO REALESTATE.GOLD.fact_property_nearby_places (
      property_id, place_type, place_name, distance_km, travel_time_min, accessibility_score
  )
  SELECT 
      id AS property_id,
      place_type,
      place_name,
      distance_km,
      travel_time_min,
      CASE 
          WHEN place_type IN ('Hospital', 'School', 'Metro Station') AND distance_km <= 2 THEN 10
          WHEN place_type IN ('Hospital', 'School', 'Metro Station') AND distance_km <= 5 THEN 8
          WHEN place_type IN ('Mall', 'Restaurant', 'Bank') AND distance_km <= 3 THEN 7
          WHEN distance_km <= 1 THEN 9
          WHEN distance_km <= 5 THEN 6
          WHEN distance_km <= 10 THEN 4
          ELSE 2
      END AS accessibility_score
  FROM REALESTATE.SILVER.property_nearby_places
  WHERE distance_km IS NOT NULL;

  RETURN 'Fact table "fact_property_nearby_places" populated successfully.';

END;
$$;
 SELECT * FROM FACT_PROPERTY_NEARBY_PLACES;




 CALL REALESTATE.GOLD.populate_fact_property_nearby_places();

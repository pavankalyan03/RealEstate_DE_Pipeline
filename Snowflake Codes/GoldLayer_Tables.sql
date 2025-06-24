-- ====================================================================
-- REAL ESTATE ANALYTICS - OPTIMIZED STAR SCHEMA IMPLEMENTATION
-- ====================================================================

-- DIMENSION TABLES
-- ====================================================================

-- CREATE OR REPLACE SCHEMA GOLD;

-- Date Dimension (Essential for time-series analysis)

CREATE OR REPLACE TABLE REALESTATE.GOLD.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name STRING NOT NULL,
    week INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name STRING NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER
);

-- Location Dimension (Snowflaked for hierarchy)
CREATE OR REPLACE TABLE REALESTATE.GOLD.dim_city (
    city_key INTEGER AUTOINCREMENT PRIMARY KEY,
    city_name STRING NOT NULL,
    state_name STRING NOT NULL,
    region STRING,
    country STRING DEFAULT 'India'
);

CREATE OR REPLACE TABLE REALESTATE.GOLD.dim_locality (
    locality_key INTEGER AUTOINCREMENT PRIMARY KEY,
    locality_name STRING NOT NULL,
    city_key INTEGER NOT NULL,
    latitude DOUBLE,
    longitude DOUBLE,
    area_type STRING, -- Commercial, Residential, Mixed
    tier STRING, -- Tier 1, Tier 2, Tier 3 locality classification
    FOREIGN KEY (city_key) REFERENCES REALESTATE.GOLD.dim_city(city_key)
);

-- Property Type Dimension
CREATE OR REPLACE TABLE REALESTATE.GOLD.dim_property_type (
    property_type_key INTEGER AUTOINCREMENT PRIMARY KEY,
    property_type STRING NOT NULL, -- Apartment, Villa, Plot, etc.
    property_category STRING NOT NULL, -- Residential, Commercial, Industrial
    is_luxury BOOLEAN DEFAULT FALSE
);

-- Property Configuration Dimension
CREATE OR REPLACE TABLE REALESTATE.GOLD.dim_configuration (
    config_key INTEGER AUTOINCREMENT PRIMARY KEY,
    config_type STRING NOT NULL, -- 1BHK, 2BHK, 3BHK, etc.
    config_category STRING -- Studio, Apartment, Villa, Plot
);


-- Property Status Dimension
CREATE OR REPLACE TABLE REALESTATE.GOLD.dim_property_status (
    status_key INTEGER AUTOINCREMENT PRIMARY KEY,
    status_name STRING NOT NULL, -- Ready to Move, Under Construction, New Launch
    is_rera_approved BOOLEAN DEFAULT FALSE,
    is_new_booking BOOLEAN DEFAULT FALSE,
    is_under_construction BOOLEAN DEFAULT FALSE,
    is_ready_to_move BOOLEAN DEFAULT FALSE,
    has_project_tag BOOLEAN DEFAULT FALSE
);

INSERT INTO REALESTATE.GOLD.dim_property_status (
    status_name, is_rera_approved, is_new_booking, is_under_construction, is_ready_to_move, has_project_tag
) VALUES (
    'Unknown', FALSE, FALSE, FALSE, FALSE, FALSE
);

INSERT INTO REALESTATE.GOLD.dim_property_status (
    status_key, status_name, is_rera_approved, is_new_booking, 
    is_under_construction, is_ready_to_move, has_project_tag
) VALUES (
    999, 'Unknown', FALSE, FALSE, FALSE, FALSE, FALSE
);

-- Builder/Seller Dimension
CREATE OR REPLACE TABLE REALESTATE.GOLD.dim_builder (
    builder_key INTEGER AUTOINCREMENT PRIMARY KEY,
    builder_name STRING NOT NULL,
    builder_type STRING, -- Individual, Company, Cooperative
    is_verified BOOLEAN DEFAULT FALSE,
    phone STRING,
    is_paid_seller BOOLEAN DEFAULT FALSE,
    reputation_score INTEGER -- 1-5 rating
);


-- Amenities Dimension
CREATE OR REPLACE TABLE REALESTATE.GOLD.dim_amenities (
    amenity_key INTEGER AUTOINCREMENT PRIMARY KEY,
    has_gated_community BOOLEAN DEFAULT FALSE,
    has_gym BOOLEAN DEFAULT FALSE,
    has_lift BOOLEAN DEFAULT FALSE,
    has_parking BOOLEAN DEFAULT FALSE,
    has_pool BOOLEAN DEFAULT FALSE,
    -- has_security BOOLEAN DEFAULT FALSE,
    -- has_garden BOOLEAN DEFAULT FALSE,
    -- has_clubhouse BOOLEAN DEFAULT FALSE,
    amenity_score INTEGER -- Calculated score based on amenities
);

-- FACT TABLES
-- ====================================================================

-- Main Fact Table for Property Listings
CREATE OR REPLACE TABLE REALESTATE.GOLD.fact_property_listings (
    -- Surrogate Key
    listing_key INTEGER AUTOINCREMENT PRIMARY KEY,
    
    -- Foreign Keys to Dimensions
    date_key INTEGER NOT NULL,
    locality_key INTEGER NOT NULL,
    property_type_key INTEGER NOT NULL,
    status_key INTEGER NOT NULL,
    builder_key INTEGER NOT NULL,
    config_key INTEGER NOT NULL,
    amenity_key INTEGER NOT NULL,
    
    -- Business Keys
    property_id STRING NOT NULL,
    listing_url STRING,
    image_url String,
    property_name STRING,
    
    -- Degenerate Dimensions (Low cardinality attributes)
    currency STRING DEFAULT 'INR',
    is_active_property BOOLEAN DEFAULT TRUE,
    is_most_contacted BOOLEAN DEFAULT FALSE,
    
    
    -- Measures/Facts
    min_price DECIMAL(15,2),
    max_price DECIMAL(15,2),
    avg_price DECIMAL(15,2),
    price_per_sqft DECIMAL(10,2),
    property_area_sqft DECIMAL(10,2),
    min_size DECIMAL(10,2),
    max_size DECIMAL(10,2),
    -- emi_amount DECIMAL(12,2),
    
    -- Dates (stored as date keys for performance)
    posted_date_key INTEGER,
    possession_date_key INTEGER,
    -- last_updated_date_key INTEGER,
    
    -- Additional metrics for analytics
    days_on_market INTEGER,
    -- price_change_percentage DECIMAL(5,2),
    -- view_count INTEGER DEFAULT 0,
    -- inquiry_count INTEGER DEFAULT 0,
    
    -- Timestamps
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    
    -- Foreign Key Constraints
    FOREIGN KEY (date_key) REFERENCES REALESTATE.GOLD.dim_date(date_key),
    FOREIGN KEY (locality_key) REFERENCES REALESTATE.GOLD.dim_locality(locality_key),
    FOREIGN KEY (property_type_key) REFERENCES REALESTATE.GOLD.dim_property_type(property_type_key),
    FOREIGN KEY (status_key) REFERENCES REALESTATE.GOLD.dim_property_status(status_key),
    FOREIGN KEY (builder_key) REFERENCES REALESTATE.GOLD.dim_builder(builder_key),
    FOREIGN KEY (config_key) REFERENCES REALESTATE.GOLD.dim_configuration(config_key),
    FOREIGN KEY (amenity_key) REFERENCES REALESTATE.GOLD.dim_amenities(amenity_key)
);


-- -- Bridge Table for Many-to-Many: Property to Nearby Places
CREATE OR REPLACE TABLE REALESTATE.GOLD.fact_property_nearby_places (
    property_id STRING NOT NULL,
    place_type STRING NOT NULL,
    place_name STRING NOT NULL,
    distance_km DECIMAL(5,2),
    travel_time_min INTEGER,
    accessibility_score INTEGER, -- 1-10 based on distance and importance
    
    PRIMARY KEY (property_id, place_type, place_name)
);

-- INDEXES FOR PERFORMANCE
-- ====================================================================


-- Clustered indexes on fact tables
ALTER TABLE REALESTATE.GOLD.fact_property_listings 
CLUSTER BY (date_key, locality_key);

-- ALTER TABLE REALESTATE.GOLD.fact_property_history 
-- CLUSTER BY (date_key, locality_key, property_id);


--======================================================================

SELECT * FROM "GOLD"."DIM_AMENITIES" LIMIT 10;
SELECT * FROM "GOLD"."DIM_BUILDER" LIMIT 10;
SELECT * FROM "GOLD"."DIM_CITY" LIMIT 10;
SELECT * FROM "GOLD"."DIM_CONFIGURATION" LIMIT 10;
SELECT * FROM "GOLD"."DIM_DATE" LIMIT 10;
SELECT * FROM "GOLD"."DIM_LOCALITY" LIMIT 10;
SELECT * FROM "GOLD"."DIM_PROPERTY_STATUS" LIMIT 3;
SELECT * FROM "GOLD"."DIM_PROPERTY_TYPE" LIMIT 10;
SELECT * FROM "GOLD"."FACT_PROPERTY_LISTINGS" ;
SELECT * FROM "GOLD"."FACT_PROPERTY_NEARBY_PLACES" LIMIT 10;


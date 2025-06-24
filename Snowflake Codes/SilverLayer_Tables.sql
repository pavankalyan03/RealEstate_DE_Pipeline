CREATE OR REPLACE SCHEMA REALESTATE.SILVER;

CREATE OR REPLACE TABLE  REALESTATE.SILVER.properties (
    address STRING,
    bathrooms INTEGER NOT NULL,
    bedrooms INTEGER NOT NULL,
    brochure_url STRING,
    city STRING,
    currency STRING,
    emi STRING,
    from_url STRING,
    id STRING,
    is_active_property BOOLEAN,
    is_most_contacted BOOLEAN,
    latitude DOUBLE,
    locality STRING,
    long_address STRING,
    longitude DOUBLE,
    max_price BIGINT,
    min_price BIGINT,
    name STRING,
    region STRING,
    seller_is_paid BOOLEAN,
    seller_name STRING,
    seller_phone STRING,
    seller_type STRING,
    state STRING,
    subtitle STRING,
    url STRING,
    description_clean STRING,
    avg_price_per_sqft DOUBLE,
    min_price_val DOUBLE,
    max_price_val DOUBLE,
    property_price DOUBLE,
    property_area_sqft DOUBLE,
    min_size DOUBLE,
    max_size DOUBLE,
    posted_date TIMESTAMP,
    possession_date DATE,
    PRIMARY KEY (id)
);

CREATE OR REPLACE TABLE  property_configurations (
    id STRING,
    config_type STRING,
    min_price_lakhs FLOAT,
    max_price_lakhs FLOAT,
    config_category STRING,
    FOREIGN KEY (id) REFERENCES properties(id)
);



CREATE OR REPLACE TABLE property_status (
    id STRING,
    is_rera_approved BOOLEAN,
    is_new_booking BOOLEAN,
    is_under_construction BOOLEAN,
    is_ready_to_move BOOLEAN,
    has_project_tag BOOLEAN,
    FOREIGN KEY (id) REFERENCES properties(id)
);


CREATE OR REPLACE TABLE property_amenities (
    id STRING,
    has_gated_community BOOLEAN NOT NULL,
    has_gym BOOLEAN NOT NULL,
    has_lift BOOLEAN NOT NULL,
    has_parking BOOLEAN NOT NULL,
    has_pool BOOLEAN NOT NULL,
    FOREIGN KEY (id) REFERENCES properties(id)
);


CREATE OR REPLACE TABLE property_nearby_places (
    id STRING,
    place_type STRING,
    place_name STRING,
    distance_km DOUBLE,
    travel_time_min INTEGER,
    FOREIGN KEY (id) REFERENCES properties(id)
);

SELECT * FROM REALESTATE.SILVER.PROPERTIES; 

--Delete the one anamoly 
DELETE FROM REALESTATE.SILVER.PROPERTIES WHERE CITY = 'Bangalore';


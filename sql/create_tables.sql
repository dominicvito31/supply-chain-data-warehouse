CREATE TABLE dim_location (
    location_key VARCHAR PRIMARY KEY,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR
);

CREATE TABLE dim_customer (
    customer_id VARCHAR PRIMARY KEY,
    location_key VARCHAR REFERENCES dim_location(location_key),
    first_name VARCHAR,
    last_name VARCHAR,
    segment VARCHAR,
    zipcode VARCHAR
);

CREATE TABLE dim_product (
    product_id VARCHAR PRIMARY KEY,
    product_name VARCHAR,
    category VARCHAR
);

CREATE TABLE dim_shipping (
    shipping_key VARCHAR PRIMARY KEY,
    shipping_mode VARCHAR,
    days_scheduled INT,
    days_real INT,
    delivery_status VARCHAR,
    late_delivery_risk BOOLEAN
);

CREATE TABLE dim_store (
    department_id VARCHAR PRIMARY KEY,
    location_key VARCHAR REFERENCES dim_location(location_key),
    department VARCHAR,
    latitude NUMERIC,
    longitude NUMERIC
);

CREATE TABLE dim_market (
    market_key VARCHAR PRIMARY KEY,
    location_key VARCHAR REFERENCES dim_location(location_key),
    market VARCHAR,
    region VARCHAR
);

CREATE TABLE dim_date (
    date_key VARCHAR PRIMARY KEY,
    date TIMESTAMP,
    year INT,
    month INT,
    day INT,
    quarter INT
);

CREATE TABLE fact_order_item (
    order_item_id VARCHAR PRIMARY KEY,
    order_id VARCHAR,
    customer_id VARCHAR REFERENCES dim_customer(customer_id),
    product_id VARCHAR REFERENCES dim_product(product_id),
    market_key VARCHAR REFERENCES dim_market(market_key),
    department_id VARCHAR REFERENCES dim_store(department_id),
    shipping_key VARCHAR REFERENCES dim_shipping(shipping_key),
    order_date_key VARCHAR REFERENCES dim_date(date_key),
    shipping_date_key VARCHAR REFERENCES dim_date(date_key),
    quantity INT,
    sales NUMERIC,
    discount_amount NUMERIC,
    discount_rate NUMERIC,
    product_price NUMERIC,
    profit NUMERIC,
    profit_ratio NUMERIC,
    benefit_per_order NUMERIC,
    order_status VARCHAR
);
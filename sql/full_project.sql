-- =====================================================
-- STAGE & DATABASE SETUP
-- =====================================================
-- Create a stage to store raw data files (CSV uploads)

create or replace stage hotel_booking;

-- Create the main database for the hotel booking pipeline

create database hotel_db;


-- =====================================================
-- RAW DATA FILE FORMAT (HOTEL BOOKINGS)
-- =====================================================
-- Define CSV format settings for the raw booking dataset

create or replace file format hotel_csv
type = 'CSV'
skip_header = 1
field_delimiter = ','
field_optionally_enclosed_by = '"'
null_if = ('NULL', 'Null', '')


-- =====================================================
-- BRONZE LAYER (RAW INGESTION)
-- =====================================================
-- The bronze layer stores raw data exactly as it arrives
-- Minimal transformations are applied at this stage

create or replace table bronze_hotel_booking (
booking_id string,
hotel_id string,
hotel_city string,
customer_id string,
customer_name string,
customer_email string,
check_in_date string,
check_out_date string,
room_type string,
num_guests string,
total_amount string,
currency string,
booking_status string
);

-- Load raw booking data from the stage into the bronze table

copy into bronze_hotel_booking
from @HOTEL_DB.PUBLIC.HOTEL_BOOKING
files = ('hotel_bookings_raw.csv')
file_format = (format_name = hotel_csv);


-- =====================================================
-- EXCHANGE RATE DATA (RAW INGESTION)
-- =====================================================
-- Define file format for the exchange rate dataset

create or replace file format exchange_rates_format
type = 'CSV'
skip_header = 1
field_delimiter = ','

-- Bronze table for raw exchange rate data

create table bronze_exchange_rates (
check_in_date_rate string,
usd_rate string,
base_currency string
) ;

-- Load exchange rate data from stage

copy into bronze_exchange_rates
from @HOTEL_DB.PUBLIC.HOTEL_BOOKING
files = ('exchange_rates.csv')
file_format = (format_name = exchange_rates_format)


-- =====================================================
-- SILVER LAYER (DATA CLEANING & STANDARDIZATION)
-- =====================================================
-- The silver layer contains cleaned and standardized data
-- Data types are corrected and invalid values are handled

create or replace table silver_hotel_booking (
booking_id varchar,
hotel_id varchar,
hotel_city varchar,
customer_id varchar,
customer_name varchar,
customer_email varchar,
check_in_date DATE,
check_out_date DATE,
room_type varchar,
num_guests integer ,
total_amount float,
currency varchar,
booking_status varchar
);


-- =====================================================
-- DATA QUALITY CHECKS
-- =====================================================
-- Check invalid or missing email addresses

select customer_email
from bronze_hotel_booking
where not (customer_email ILIKE '%@%') OR customer_email IS NULL;

-- Observation:
-- Some records contain 'invalid-email' or NULL values

-- Check negative booking amounts

select total_amount 
from bronze_hotel_booking
where try_to_number(total_amount) < 0;

-- Observation:
-- Negative values exist and will be converted to positive

-- Check logical errors in booking dates

select check_in_date, check_out_date
from bronze_hotel_booking
where try_to_date(check_out_date) < try_to_date(check_in_date);

-- Observation:
-- Some records have check-in dates later than check-out dates

-- Inspect booking status values

select distinct booking_status
from bronze_hotel_booking;

-- Observation:
-- Inconsistent values exist such as:
-- Confirmed, confirmeeed, cancelled, no-show, NULL


-- =====================================================
-- TRANSFORM RAW DATA INTO SILVER LAYER
-- =====================================================
-- Apply cleaning, formatting, and validation rules

insert into silver_hotel_booking
SELECT
    booking_id,
    hotel_id,
    initcap(trim(hotel_city)) AS hotel_city,
    customer_id,
    initcap(trim(customer_name)) AS customer_name,
    CASE WHEN customer_email = 'invalid-email' THEN NULL
         ELSE customer_email END AS customer_email,
    TRY_TO_DATE(NULLIF(check_in_date, '')) AS check_in_date, 
    TRY_TO_DATE(NULLIF(check_out_date, '')) AS check_out_date,
    room_type,
    num_guests,
    ABS(TRY_TO_NUMBER(total_amount)) AS total_amount,
    currency,
    CASE WHEN lower(booking_status) in ('confirmeeed', 'confirmed') THEN 'Confirmed'
    ELSE booking_status END AS booking_status
FROM bronze_hotel_booking
WHERE 
    TRY_TO_DATE(check_in_date) IS NOT NULL
    AND TRY_TO_DATE(check_out_date) IS NOT NULL
    AND TRY_TO_DATE(check_in_date) <= TRY_TO_DATE (check_out_date);


-- =====================================================
-- SILVER LAYER FOR EXCHANGE RATES
-- =====================================================
-- Convert exchange rate dataset to structured types

create table silver_exchange_rates (
check_in_date_rate DATE,
usd_rate FLOAT,
base_currency VARCHAR
)

-- Populate the silver exchange rate table

insert into silver_exchange_rates
SELECT 
    check_in_date_rate,
    usd_rate,
    base_currency
FROM bronze_exchange_rates;


-- =====================================================
-- GOLD LAYER (ANALYTICS READY DATA)
-- =====================================================
-- Final dataset prepared for analytics and reporting
-- Includes currency normalization to USD

CREATE OR REPLACE TABLE gold_hotel_booking AS
SELECT
    a.booking_id,
    a.hotel_id,
    a.hotel_city,
    a.customer_id,
    a.customer_name,
    a.customer_email,
    a.check_in_date,
    a.check_out_date,
    a.room_type,
    a.num_guests,
    a.total_amount,
    ROUND(a.total_amount * COALESCE(b.usd_rate, 1.0), 2) AS usd_total_amount,
    a.currency,
    a.booking_status
FROM silver_hotel_booking AS a
LEFT JOIN silver_exchange_rates AS b
    ON a.check_in_date = b.check_in_date_rate 
    AND COALESCE(a.currency, 'USD') = b.base_currency;

CREATE SCHEMA IF NOT EXISTS BL_3NF;

SET search_path = BL_3NF;


CREATE OR REPLACE PROCEDURE CREATE_TABLE_CE_ORDER_PRIORITIES()
LANGUAGE plpgsql
AS $$
DECLARE
    num_rows_affected integer; log_message TEXT;
BEGIN
    CREATE TABLE IF NOT EXISTS BL_3NF.CE_ORDER_PRIORITIES (
		    ORDER_PRIORITY_ID BIGINT PRIMARY KEY,
		    ORDER_PRIORITY_SRC_ID VARCHAR(32),
		    SOURCE_SYSTEM VARCHAR(32),
		    SOURCE_TABLE VARCHAR(32),
		    ORDER_PRIORITY_NAME VARCHAR(255),
		    ORDER_PRIORITY_DESCRIPTION VARCHAR(255),
		    UPDATE_DT DATE
		);    
   	CREATE SEQUENCE IF NOT EXISTS BL_3NF.order_priorities_seq
		INCREMENT BY 1
		MINVALUE 1
		MAXVALUE 9223372036854775807
		START 1
		CACHE 1
		NO CYCLE;
	
	INSERT INTO BL_3NF.CE_ORDER_PRIORITIES
	    (ORDER_PRIORITY_ID,
	    ORDER_PRIORITY_SRC_ID,
	    SOURCE_SYSTEM,
	    SOURCE_TABLE,
	    ORDER_PRIORITY_NAME,
	    ORDER_PRIORITY_DESCRIPTION,
	    UPDATE_DT)
	SELECT
	    -1 AS ORDER_PRIORITY_ID,
	    'n.a' AS ORDER_PRIORITY_SRC_ID,
	    'MANUAL' AS SOURCE_SYSTEM,
	    'MANUAL' AS SOURCE_TABLE,
	    'n.a' AS ORDER_PRIORITY_NAME,
	    'n.a' AS ORDER_PRIORITY_DESCRIPTION,
	    '1900-01-01' AS UPDATE_DT
	WHERE NOT EXISTS (
	    SELECT 1
	    FROM BL_3NF.CE_ORDER_PRIORITIES
	    WHERE ORDER_PRIORITY_ID = -1
	);
	GET DIAGNOSTICS num_rows_affected = ROW_COUNT; 
    log_message := 'Procedure CREATE_TABLE_CE_ORDER_PRIORITIES completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, num_rows_affected, log_message) VALUES ('CREATE_TABLE_CE_ORDER_PRIORITIES', num_rows_affected, log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in CREATE_TABLE_CE_ORDER_PRIORITIES: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('CREATE_TABLE_CE_ORDER_PRIORITIES', log_message);
        RAISE;
END;
$$;






CREATE OR REPLACE PROCEDURE CREATE_TABLE_CE_PROMOTION_TYPES()
LANGUAGE plpgsql
AS $$
DECLARE
    num_rows_affected integer; log_message TEXT;
BEGIN
    CREATE TABLE IF NOT EXISTS BL_3NF.CE_PROMOTION_TYPES (
        PROMOTION_TYPE_ID BIGINT PRIMARY KEY,
        PROMOTION_TYPE_SRC_ID VARCHAR(32),
        SOURCE_SYSTEM VARCHAR(32),
        SOURCE_TABLE VARCHAR(32),
        PROMOTION_TYPE_NAME VARCHAR(255),
        SALE_PERCENTAGE DECIMAL(4, 2),
        UPDATE_DT DATE
    );


    CREATE SEQUENCE IF NOT EXISTS BL_3NF.promotion_types_seq
        INCREMENT BY 1
        MINVALUE 1
        MAXVALUE 9223372036854775807
        START 1
        CACHE 1
        NO CYCLE;
           
	INSERT INTO BL_3NF.CE_PROMOTION_TYPES 
	    (PROMOTION_TYPE_ID,
	    PROMOTION_TYPE_SRC_ID,
	    SOURCE_SYSTEM,
	    SOURCE_TABLE,
	    PROMOTION_TYPE_NAME,
	    SALE_PERCENTAGE,
	    UPDATE_DT)
	SELECT
	    -1 AS PROMOTION_TYPE_ID,
	    'n.a' AS PROMOTION_TYPE_SRC_ID,
	    'MANUAL' AS SOURCE_SYSTEM,
	    'MANUAL' AS SOURCE_TABLE,
	    'n.a' AS PROMOTION_TYPE_NAME,
	    -1 AS SALE_PERCENTAGE,
	    '1900-01-01' AS UPDATE_DT
	WHERE NOT EXISTS (
	    SELECT 1
	    FROM BL_3NF.CE_PROMOTION_TYPES
	    WHERE PROMOTION_TYPE_ID = -1
	);
	GET DIAGNOSTICS num_rows_affected = ROW_COUNT; 
    log_message := 'Procedure CREATE_TABLE_CE_PROMOTION_TYPES completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, num_rows_affected, log_message) VALUES ('CREATE_TABLE_CE_PROMOTION_TYPES', num_rows_affected, log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in CREATE_TABLE_CE_PROMOTION_TYPES: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('CREATE_TABLE_CE_PROMOTION_TYPES', log_message);
        RAISE;
END;
$$;



CREATE OR REPLACE PROCEDURE CREATE_TABLE_CE_CUSTOMER_TYPES()
LANGUAGE plpgsql
AS $$
DECLARE
    num_rows_affected integer; log_message TEXT;
BEGIN
    CREATE TABLE IF NOT EXISTS BL_3NF.CE_CUSTOMER_TYPES (
        CUSTOMER_TYPE_ID BIGINT PRIMARY KEY,
        CUSTOMER_TYPE_SRC_ID VARCHAR(32),
        SOURCE_SYSTEM VARCHAR(32),
        SOURCE_TABLE VARCHAR(32),
        CUSTOMER_TYPE_NAME VARCHAR(255),
        CUSTOMER_TYPE_DESCRIPTION VARCHAR(255),
        UPDATE_DT DATE
    );


    CREATE SEQUENCE IF NOT EXISTS BL_3NF.customer_types_seq
        INCREMENT BY 1
        MINVALUE 1
        MAXVALUE 9223372036854775807
        START 1
        CACHE 1
        NO CYCLE;

	INSERT INTO BL_3NF.CE_CUSTOMER_TYPES
	    (CUSTOMER_TYPE_ID,
	    CUSTOMER_TYPE_SRC_ID,
	    SOURCE_SYSTEM,
	    SOURCE_TABLE,
	    CUSTOMER_TYPE_NAME,
	    CUSTOMER_TYPE_DESCRIPTION,
	    UPDATE_DT)
	SELECT
	    -1 AS CUSTOMER_TYPE_ID,
	    'n.a' AS CUSTOMER_TYPE_SRC_ID,
	    'MANUAL' AS SOURCE_SYSTEM,
	    'MANUAL' AS SOURCE_TABLE,
	    'n.a' AS CUSTOMER_TYPE_NAME,
	    'n.a' AS CUSTOMER_TYPE_DESCRIPTION,
	    '1900-01-01' AS UPDATE_DT
	WHERE NOT EXISTS (
	    SELECT 1
	    FROM BL_3NF.CE_CUSTOMER_TYPES
	    WHERE CUSTOMER_TYPE_ID = -1
	);           
    GET DIAGNOSTICS num_rows_affected = ROW_COUNT; 
    log_message := 'Procedure CREATE_TABLE_CE_CUSTOMER_TYPES completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, num_rows_affected, log_message) VALUES ('CREATE_TABLE_CE_CUSTOMER_TYPES', num_rows_affected, log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in CREATE_TABLE_CE_CUSTOMER_TYPES: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('CREATE_TABLE_CE_CUSTOMER_TYPES', log_message);
        RAISE;
END;
$$;



CREATE OR REPLACE PROCEDURE CREATE_TABLE_CE_PAYMENT_METHODS()
LANGUAGE plpgsql
AS $$
DECLARE
    num_rows_affected integer; log_message TEXT;
BEGIN
    CREATE TABLE IF NOT EXISTS BL_3NF.CE_PAYMENT_METHODS (
        PAYMENT_METHOD_ID BIGINT PRIMARY KEY,
        PAYMENT_METHOD_SRC_ID VARCHAR(32),
        SOURCE_SYSTEM VARCHAR(32),
        SOURCE_TABLE VARCHAR(32),
        PAYMENT_METHOD_NAME VARCHAR(255),
        PAYMENT_METHOD_DESCRIPTION VARCHAR(255),
        UPDATE_DT DATE
    );


    CREATE SEQUENCE IF NOT EXISTS BL_3NF.payment_methods_seq
        INCREMENT BY 1
        MINVALUE 1
        MAXVALUE 9223372036854775807
        START 1
        CACHE 1
        NO CYCLE;
	
	INSERT INTO BL_3NF.CE_PAYMENT_METHODS 
	    (PAYMENT_METHOD_ID,
	    PAYMENT_METHOD_SRC_ID,
	    SOURCE_SYSTEM,
	    SOURCE_TABLE,
	    PAYMENT_METHOD_NAME,
	    PAYMENT_METHOD_DESCRIPTION,
	    UPDATE_DT)
	SELECT
	    -1 AS PAYMENT_METHOD_ID,
	    'n.a' AS PAYMENT_METHOD_SRC_ID,
	    'MANUAL' AS SOURCE_SYSTEM,
	    'MANUAL' AS SOURCE_TABLE,
	    'n.a' AS PAYMENT_METHOD_NAME,
	    'n.a' AS PAYMENT_METHOD_DESCRIPTION,
	    '1900-01-01' AS UPDATE_DT
	WHERE NOT EXISTS (
	    SELECT 1
	    FROM BL_3NF.CE_PAYMENT_METHODS
	    WHERE PAYMENT_METHOD_ID = -1
	);    
	GET DIAGNOSTICS num_rows_affected = ROW_COUNT; 
    log_message := 'Procedure CREATE_TABLE_CE_PAYMENT_METHODS completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, num_rows_affected, log_message) VALUES ('CREATE_TABLE_CE_PAYMENT_METHODS', num_rows_affected, log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in CREATE_TABLE_CE_PAYMENT_METHODS: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('CREATE_TABLE_CE_PAYMENT_METHODS', log_message);
        RAISE;
END;
$$;



CREATE OR REPLACE PROCEDURE CREATE_TABLE_CE_SALES_CHANNELS()
LANGUAGE plpgsql
AS $$
DECLARE
    num_rows_affected integer; log_message TEXT;
BEGIN
    CREATE TABLE IF NOT EXISTS BL_3NF.CE_SALES_CHANNELS (
        SALES_CHANNEL_ID BIGINT PRIMARY KEY,
        SALES_CHANNEL_SRC_ID VARCHAR(32),
        SOURCE_SYSTEM VARCHAR(32),
        SOURCE_TABLE VARCHAR(32),
        SALES_CHANNEL_NAME VARCHAR(255),
        UPDATE_DT DATE
    );


    CREATE SEQUENCE IF NOT EXISTS BL_3NF.sales_channels_seq
        INCREMENT BY 1
        MINVALUE 1
        MAXVALUE 9223372036854775807
        START 1
        CACHE 1
        NO CYCLE;

	INSERT INTO BL_3NF.CE_SALES_CHANNELS
	    (SALES_CHANNEL_ID,
	    SALES_CHANNEL_SRC_ID,
	    SOURCE_SYSTEM,
	    SOURCE_TABLE,
	    SALES_CHANNEL_NAME,
	    UPDATE_DT)
	SELECT
	    -1 AS SALES_CHANNEL_ID,
	    'n.a' AS SALES_CHANNEL_SRC_ID,
	    'MANUAL' AS SOURCE_SYSTEM,
	    'MANUAL' AS SOURCE_TABLE,
	    'n.a' AS SALES_CHANNEL_NAME,
	    '1900-01-01' AS UPDATE_DT
	WHERE NOT EXISTS (
	    SELECT 1
	    FROM BL_3NF.CE_SALES_CHANNELS
	    WHERE SALES_CHANNEL_ID = -1
	);
	GET DIAGNOSTICS num_rows_affected = ROW_COUNT; 
    log_message := 'Procedure CREATE_TABLE_CE_SALES_CHANNELS completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, num_rows_affected, log_message) VALUES ('CREATE_TABLE_CE_SALES_CHANNELS', num_rows_affected, log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in CREATE_TABLE_CE_SALES_CHANNELS: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('CREATE_TABLE_CE_SALES_CHANNELS', log_message);
        RAISE;
END;
$$;


CREATE OR REPLACE PROCEDURE CREATE_TABLE_CE_ORDERING_WAYS()
LANGUAGE plpgsql
AS $$
DECLARE
    num_rows_affected integer; log_message TEXT;
BEGIN
    CREATE TABLE IF NOT EXISTS BL_3NF.CE_ORDERING_WAYS (
        ORDERING_WAY_ID BIGINT PRIMARY KEY,
        ORDERING_WAY_SRC_ID VARCHAR(32),
        SOURCE_SYSTEM VARCHAR(32),
        SOURCE_TABLE VARCHAR(32),
        ORDERING_WAY_NAME VARCHAR(255),
        ORDERING_WAY_DESCRIPTION VARCHAR(255),
        UPDATE_DT DATE
    );


    CREATE SEQUENCE IF NOT EXISTS BL_3NF.ordering_ways_seq
        INCREMENT BY 1
        MINVALUE 1
        MAXVALUE 9223372036854775807
        START 1
        CACHE 1
        NO CYCLE;
           
	INSERT INTO BL_3NF.CE_ORDERING_WAYS 
	    (ORDERING_WAY_ID,
	    ORDERING_WAY_SRC_ID,
	    SOURCE_SYSTEM,
	    SOURCE_TABLE,
	    ORDERING_WAY_NAME,
	    ORDERING_WAY_DESCRIPTION,
	    UPDATE_DT)
	SELECT
	    -1 AS ORDERING_WAY_ID,
	    'n.a' AS ORDERING_WAY_SRC_ID,
	    'MANUAL' AS SOURCE_SYSTEM,
	    'MANUAL' AS SOURCE_TABLE,
	    'n.a' AS ORDERING_WAY_NAME,
	    'n.a' AS ORDERING_WAY_DESCRIPTION,
	    '1900-01-01' AS UPDATE_DT
	WHERE NOT EXISTS (
	    SELECT 1
	    FROM BL_3NF.CE_ORDERING_WAYS
	    WHERE ORDERING_WAY_ID = -1
	);           
    GET DIAGNOSTICS num_rows_affected = ROW_COUNT; 
    log_message := 'Procedure CREATE_TABLE_CE_ORDERING_WAYS completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, num_rows_affected, log_message) VALUES ('CREATE_TABLE_CE_ORDERING_WAYS', num_rows_affected, log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in CREATE_TABLE_CE_ORDERING_WAYS: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('CREATE_TABLE_CE_ORDERING_WAYS', log_message);
        RAISE;
END;
$$;

CREATE OR REPLACE PROCEDURE CREATE_TABLE_CE_ITEM_CATEGORIES()
LANGUAGE plpgsql
AS $$
DECLARE
    num_rows_affected integer; log_message TEXT;
BEGIN
    CREATE TABLE IF NOT EXISTS BL_3NF.CE_ITEM_CATEGORIES (
        ITEM_CATEGORY_ID BIGINT PRIMARY KEY,
        ITEM_CATEGORY_SRC_ID VARCHAR(32),
        SOURCE_SYSTEM VARCHAR(32),
        SOURCE_TABLE VARCHAR(32),
        ITEM_CATEGORY_NAME VARCHAR(255),
        UPDATE_DT DATE
    );


    CREATE SEQUENCE IF NOT EXISTS BL_3NF.item_categories_seq
        INCREMENT BY 1
        MINVALUE 1
        MAXVALUE 9223372036854775807
        START 1
        CACHE 1
        NO CYCLE;

	INSERT INTO BL_3NF.CE_ITEM_CATEGORIES 
	    (ITEM_CATEGORY_ID,
	    ITEM_CATEGORY_SRC_ID,
	    SOURCE_SYSTEM,
	    SOURCE_TABLE,
	    ITEM_CATEGORY_NAME,
	    UPDATE_DT)
	SELECT
	    -1 AS ITEM_CATEGORY_ID,
	    'n.a' AS ITEM_CATEGORY_SRC_ID,
	    'MANUAL' AS SOURCE_SYSTEM,
	    'MANUAL' AS SOURCE_TABLE,
	    'n.a' AS ITEM_CATEGORY_NAME,
	    '1900-01-01' AS UPDATE_DT
	WHERE NOT EXISTS (
	    SELECT 1
	    FROM BL_3NF.CE_ITEM_CATEGORIES
	    WHERE ITEM_CATEGORY_ID = -1
	);          
    GET DIAGNOSTICS num_rows_affected = ROW_COUNT; 
    log_message := 'Procedure CREATE_TABLE_CE_ITEM_CATEGORIES completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, num_rows_affected, log_message) VALUES ('CREATE_TABLE_CE_ITEM_CATEGORIES', num_rows_affected, log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in CREATE_TABLE_CE_ITEM_CATEGORIES: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('CREATE_TABLE_CE_ITEM_CATEGORIES', log_message);
        RAISE;
END;
$$;



CREATE OR REPLACE PROCEDURE CREATE_TABLE_CE_ITEM_TYPES()
LANGUAGE plpgsql
AS $$
DECLARE
    num_rows_affected integer; log_message TEXT;
BEGIN
    CREATE TABLE IF NOT EXISTS BL_3NF.CE_ITEM_TYPES (
        ITEM_TYPE_ID BIGINT PRIMARY KEY,
        ITEM_TYPE_SRC_ID VARCHAR(32),
        SOURCE_SYSTEM VARCHAR(32),
        SOURCE_TABLE VARCHAR(32),
        ITEM_TYPE_NAME VARCHAR(255),
        ITEM_CATEGORY_ID BIGINT REFERENCES BL_3NF.CE_ITEM_CATEGORIES(ITEM_CATEGORY_ID),
        UPDATE_DT DATE
    );


    CREATE SEQUENCE IF NOT EXISTS BL_3NF.item_types_seq
        INCREMENT BY 1
        MINVALUE 1
        MAXVALUE 9223372036854775807
        START 1
        CACHE 1
        NO CYCLE;
   
	INSERT INTO BL_3NF.CE_ITEM_TYPES 
	    (ITEM_TYPE_ID,
	    ITEM_TYPE_SRC_ID,
	    SOURCE_SYSTEM,
	    SOURCE_TABLE,
	    ITEM_TYPE_NAME,
	    ITEM_CATEGORY_ID,
	    UPDATE_DT)
	SELECT
	    -1 AS ITEM_TYPE_ID,
	    'n.a' AS ITEM_TYPE_SRC_ID,
	    'MANUAL' AS SOURCE_SYSTEM,
	    'MANUAL' AS SOURCE_TABLE,
	    'n.a' AS ITEM_TYPE_NAME,
	    -1 AS ITEM_CATEGORY_ID,
	    '1900-01-01' AS UPDATE_DT
	WHERE NOT EXISTS (
	    SELECT 1
	    FROM BL_3NF.CE_ITEM_TYPES
	    WHERE ITEM_TYPE_ID = -1
	);           
    GET DIAGNOSTICS num_rows_affected = ROW_COUNT; 
    log_message := 'Procedure CREATE_TABLE_CE_ITEM_TYPES completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, num_rows_affected, log_message) VALUES ('CREATE_TABLE_CE_ITEM_TYPES', num_rows_affected, log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in CREATE_TABLE_CE_ITEM_TYPES: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('CREATE_TABLE_CE_ITEM_TYPES', log_message);
        RAISE;
END;
$$;



CREATE OR REPLACE PROCEDURE CREATE_TABLE_CE_ITEMS()
LANGUAGE plpgsql
AS $$
DECLARE
    num_rows_affected integer; log_message TEXT;
BEGIN
    CREATE TABLE IF NOT EXISTS BL_3NF.CE_ITEMS (
        ITEM_ID BIGINT PRIMARY KEY,
        ITEM_SRC_ID VARCHAR(32),
        SOURCE_SYSTEM VARCHAR(32),
        SOURCE_TABLE VARCHAR(32),
        ITEM_NAME VARCHAR(255),
        ITEM_TYPE_ID BIGINT REFERENCES BL_3NF.CE_ITEM_TYPES(ITEM_TYPE_ID),
        UPDATE_DT DATE
    );


    CREATE SEQUENCE IF NOT EXISTS BL_3NF.items_seq
        INCREMENT BY 1
        MINVALUE 1
        MAXVALUE 9223372036854775807
        START 1
        CACHE 1
        NO CYCLE;
       
    CREATE INDEX IF NOT EXISTS idx_item_name_item_type_id ON bl_3nf.CE_ITEMS (ITEM_NAME, ITEM_TYPE_ID);
	    
	INSERT INTO BL_3NF.CE_ITEMS
	    (ITEM_ID,
	    ITEM_SRC_ID,
	    SOURCE_SYSTEM,
	    SOURCE_TABLE,
	    ITEM_NAME,
	    ITEM_TYPE_ID,
	    UPDATE_DT)
	SELECT
	    -1 AS ITEM_ID,
	    'n.a' AS ITEM_SRC_ID,
	    'MANUAL' AS SOURCE_SYSTEM,
	    'MANUAL' AS SOURCE_TABLE,
	    'n.a' AS ITEM_NAME,
	    -1 AS ITEM_TYPE_ID,
	    '1900-01-01' AS UPDATE_DT
	WHERE NOT EXISTS (
	    SELECT 1
	    FROM BL_3NF.CE_ITEMS
	    WHERE ITEM_ID = -1
	);           
    GET DIAGNOSTICS num_rows_affected = ROW_COUNT; 
    log_message := 'Procedure CREATE_TABLE_CE_ITEMS completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, num_rows_affected, log_message) VALUES ('CREATE_TABLE_CE_ITEMS', num_rows_affected, log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in CREATE_TABLE_CE_ITEMS: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('CREATE_TABLE_CE_ITEMS', log_message);
        RAISE;
END;
$$;


CREATE OR REPLACE PROCEDURE CREATE_TABLE_CE_REGIONS()
LANGUAGE plpgsql
AS $$
DECLARE
    num_rows_affected integer; log_message TEXT;
BEGIN
    CREATE TABLE IF NOT EXISTS BL_3NF.CE_REGIONS (
        REGION_ID BIGINT PRIMARY KEY,
        REGION_SRC_ID VARCHAR(32),
        SOURCE_SYSTEM VARCHAR(32),
        SOURCE_TABLE VARCHAR(32),
        REGION_NAME VARCHAR(255),
        UPDATE_DT DATE
    );


    CREATE SEQUENCE IF NOT EXISTS BL_3NF.regions_seq
        INCREMENT BY 1
        MINVALUE 1
        MAXVALUE 9223372036854775807
        START 1
        CACHE 1
        NO CYCLE;
    
	INSERT INTO BL_3NF.CE_REGIONS 
	    (REGION_ID,
	    REGION_SRC_ID,
	    SOURCE_SYSTEM,
	    SOURCE_TABLE,
	    REGION_NAME,
	    UPDATE_DT)
	SELECT
	    -1 AS REGION_ID,
	    'n.a' AS REGION_SRC_ID,
	    'MANUAL' AS SOURCE_SYSTEM,
	    'MANUAL' AS SOURCE_TABLE,
	    'n.a' AS REGION_NAME,
	    '1900-01-01' AS UPDATE_DT
	WHERE NOT EXISTS (
	    SELECT 1
	    FROM BL_3NF.CE_REGIONS
	    WHERE REGION_ID = -1
	);           
    GET DIAGNOSTICS num_rows_affected = ROW_COUNT; 
    log_message := 'Procedure CREATE_TABLE_CE_REGIONS completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, num_rows_affected, log_message) VALUES ('CREATE_TABLE_CE_REGIONS', num_rows_affected, log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in CREATE_TABLE_CE_REGIONS: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('CREATE_TABLE_CE_REGIONS', log_message);
        RAISE;
END;
$$;


CREATE OR REPLACE PROCEDURE CREATE_TABLE_CE_COUNTRIES()
LANGUAGE plpgsql
AS $$
DECLARE
    num_rows_affected integer; log_message TEXT;
BEGIN
    CREATE TABLE IF NOT EXISTS BL_3NF.CE_COUNTRIES (
        COUNTRY_ID BIGINT PRIMARY KEY,
        COUNTRY_SRC_ID VARCHAR(32),
        SOURCE_SYSTEM VARCHAR(32),
        SOURCE_TABLE VARCHAR(32),
        COUNTRY_NAME VARCHAR(255),
        REGION_ID BIGINT REFERENCES BL_3NF.CE_REGIONS(REGION_ID),
        UPDATE_DT DATE
    );


    CREATE SEQUENCE IF NOT EXISTS BL_3NF.countries_seq
        INCREMENT BY 1
        MINVALUE 1
        MAXVALUE 9223372036854775807
        START 1
        CACHE 1
        NO CYCLE;
    
	INSERT INTO BL_3NF.CE_COUNTRIES 
	    (COUNTRY_ID,
	    COUNTRY_SRC_ID,
	    SOURCE_SYSTEM,
	    SOURCE_TABLE,
	    COUNTRY_NAME,
	    REGION_ID,
	    UPDATE_DT)
	SELECT
	    -1 AS COUNTRY_ID,
	    'n.a' AS COUNTRY_SRC_ID,
	    'MANUAL' AS SOURCE_SYSTEM,
	    'MANUAL' AS SOURCE_TABLE,
	    'n.a' AS COUNTRY_NAME,
	    -1 AS REGION_ID,
	    '1900-01-01' AS UPDATE_DT
	WHERE NOT EXISTS (
	    SELECT 1
	    FROM BL_3NF.CE_COUNTRIES
	    WHERE COUNTRY_ID = -1
	);           
    GET DIAGNOSTICS num_rows_affected = ROW_COUNT;  
    log_message := 'Procedure CREATE_TABLE_CE_COUNTRIES completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, num_rows_affected, log_message) VALUES ('CREATE_TABLE_CE_COUNTRIES', num_rows_affected, log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in CREATE_TABLE_CE_COUNTRIES: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('CREATE_TABLE_CE_COUNTRIES', log_message);
        RAISE;
END;
$$;


CREATE OR REPLACE PROCEDURE CREATE_TABLE_CE_CITIES()
LANGUAGE plpgsql
AS $$
DECLARE
    num_rows_affected integer; log_message TEXT;
BEGIN
    CREATE TABLE IF NOT EXISTS BL_3NF.CE_CITIES (
        CITY_ID BIGINT PRIMARY KEY,
        CITY_SRC_ID VARCHAR(32),
        SOURCE_SYSTEM VARCHAR(32),
        SOURCE_TABLE VARCHAR(32),
        CITY_NAME VARCHAR(255),
        COUNTRY_ID BIGINT REFERENCES BL_3NF.CE_COUNTRIES(COUNTRY_ID),
        UPDATE_DT DATE
    );


    CREATE SEQUENCE IF NOT EXISTS BL_3NF.cities_seq
        INCREMENT BY 1
        MINVALUE 1
        MAXVALUE 9223372036854775807
        START 1
        CACHE 1
        NO CYCLE;
    
	INSERT INTO BL_3NF.CE_CITIES 
	    (CITY_ID,
	    CITY_SRC_ID,
	    SOURCE_SYSTEM,
	    SOURCE_TABLE,
	    CITY_NAME,
	    COUNTRY_ID,
	    UPDATE_DT)
	SELECT
	    -1 AS CITY_ID,
	    'n.a' AS CITY_SRC_ID,
	    'MANUAL' AS SOURCE_SYSTEM,
	    'MANUAL' AS SOURCE_TABLE,
	    'n.a' AS CITY_NAME,
	    -1 AS COUNTRY_ID,
	    '1900-01-01' AS UPDATE_DT
	WHERE NOT EXISTS (
	    SELECT 1
	    FROM BL_3NF.CE_CITIES
	    WHERE CITY_ID = -1
	);
           
    GET DIAGNOSTICS num_rows_affected = ROW_COUNT; 
    log_message := 'Procedure CREATE_TABLE_CE_CITIES completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, num_rows_affected, log_message) VALUES ('CREATE_TABLE_CE_CITIES', num_rows_affected, log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in CREATE_TABLE_CE_CITIES: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('CREATE_TABLE_CE_CITIES', log_message);
        RAISE;
END;
$$;



CREATE OR REPLACE PROCEDURE CREATE_TABLE_CE_ADDRESSES()
LANGUAGE plpgsql
AS $$
DECLARE
    num_rows_affected integer; log_message TEXT;
BEGIN
    CREATE TABLE IF NOT EXISTS BL_3NF.CE_ADDRESSES (
        ADDRESS_ID BIGINT PRIMARY KEY,
        ADDRESS_SRC_ID VARCHAR(32),
        SOURCE_SYSTEM VARCHAR(32),
        SOURCE_TABLE VARCHAR(32),
        ADDRESS_NAME VARCHAR(255),
        CITY_ID BIGINT REFERENCES BL_3NF.CE_CITIES(CITY_ID),
        UPDATE_DT DATE
    );


    CREATE SEQUENCE IF NOT EXISTS BL_3NF.addresses_seq
        INCREMENT BY 1
        MINVALUE 1
        MAXVALUE 9223372036854775807
        START 1
        CACHE 1
        NO CYCLE;

    CREATE INDEX IF NOT EXISTS idx_address_name ON bl_3nf.CE_ADDRESSES (ADDRESS_NAME);

	INSERT INTO BL_3NF.CE_ADDRESSES 
	    (ADDRESS_ID,
	    ADDRESS_SRC_ID,
	    SOURCE_SYSTEM,
	    SOURCE_TABLE,
	    ADDRESS_NAME,
	    CITY_ID,
	    UPDATE_DT)
	SELECT
	    -1 AS ADDRESS_ID,
	    'n.a' AS ADDRESS_SRC_ID,
	    'MANUAL' AS SOURCE_SYSTEM,
	    'MANUAL' AS SOURCE_TABLE,
	    'n.a' AS ADDRESS_NAME,
	    -1 AS CITY_ID,
	    '1900-01-01' AS UPDATE_DT
	WHERE NOT EXISTS (
	    SELECT 1
	    FROM BL_3NF.CE_ADDRESSES
	    WHERE ADDRESS_ID = -1
	);           
    GET DIAGNOSTICS num_rows_affected = ROW_COUNT; 
    log_message := 'Procedure CREATE_TABLE_CE_ADDRESSES completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, num_rows_affected, log_message) VALUES ('CREATE_TABLE_CE_ADDRESSES', num_rows_affected, log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in CREATE_TABLE_CE_ADDRESSES: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('CREATE_TABLE_CE_ADDRESSES', log_message);
        RAISE;
END;
$$;



CREATE OR REPLACE PROCEDURE CREATE_TABLE_CE_STORE_SIZES()
LANGUAGE plpgsql
AS $$
DECLARE
    num_rows_affected integer; log_message TEXT;
BEGIN
    CREATE TABLE IF NOT EXISTS BL_3NF.CE_STORE_SIZES (
        STORE_SIZE_ID BIGINT PRIMARY KEY,
        STORE_SIZE_SRC_ID VARCHAR(32),
        SOURCE_SYSTEM VARCHAR(32),
        SOURCE_TABLE VARCHAR(32),
        STORE_SIZE_NAME VARCHAR(255),
        STORE_SIZE_DESCRIPTION VARCHAR(255),
        UPDATE_DT DATE
    );


    CREATE SEQUENCE IF NOT EXISTS BL_3NF.store_sizes_seq
        INCREMENT BY 1
        MINVALUE 1
        MAXVALUE 9223372036854775807
        START 1
        CACHE 1
        NO CYCLE;
    
	INSERT INTO BL_3NF.CE_STORE_SIZES 
	    (STORE_SIZE_ID,
	    STORE_SIZE_SRC_ID,
	    SOURCE_SYSTEM,
	    SOURCE_TABLE,
	    STORE_SIZE_NAME,
	    STORE_SIZE_DESCRIPTION,
	    UPDATE_DT)
	SELECT
	    -1 AS STORE_SIZE_ID,
	    'n.a' AS STORE_SIZE_SRC_ID,
	    'MANUAL' AS SOURCE_SYSTEM,
	    'MANUAL' AS SOURCE_TABLE,
	    'n.a' AS STORE_SIZE_NAME,
	    'n.a' AS STORE_SIZE_DESCRIPTION,
	    '1900-01-01' AS UPDATE_DT
	WHERE NOT EXISTS (
	    SELECT 1
	    FROM BL_3NF.CE_STORE_SIZES
	    WHERE STORE_SIZE_ID = -1
	);           
    GET DIAGNOSTICS num_rows_affected = ROW_COUNT; 
    log_message := 'Procedure CREATE_TABLE_CE_STORE_SIZES completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, num_rows_affected, log_message) VALUES ('CREATE_TABLE_CE_STORE_SIZES', num_rows_affected, log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in CREATE_TABLE_CE_STORE_SIZES: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('CREATE_TABLE_CE_STORE_SIZES', log_message);
        RAISE;
END;
$$;




CREATE OR REPLACE PROCEDURE CREATE_TABLE_CE_STORES_SCD()
LANGUAGE plpgsql
AS $$
DECLARE
    num_rows_affected integer; log_message TEXT;
BEGIN
    CREATE TABLE IF NOT EXISTS BL_3NF.CE_STORES_SCD (
        STORE_ID BIGINT,
        STORE_SRC_ID VARCHAR(32),
        SOURCE_SYSTEM VARCHAR(32),
        SOURCE_TABLE VARCHAR(32),
        STORE_ADDRESS_ID BIGINT REFERENCES BL_3NF.CE_ADDRESSES(ADDRESS_ID),
        STORE_SIZE_ID BIGINT REFERENCES BL_3NF.CE_STORE_SIZES(STORE_SIZE_ID),
        START_DT DATE,
        END_DT DATE,
        IS_ACTIVE BOOLEAN,
        INSERT_DT DATE,
        PRIMARY KEY (STORE_ID, START_DT)
    );


    CREATE SEQUENCE IF NOT EXISTS BL_3NF.stores_seq
        INCREMENT BY 1
        MINVALUE 1
        MAXVALUE 9223372036854775807
        START 1
        CACHE 1
        NO CYCLE;
    
    CREATE INDEX IF NOT EXISTS idx_store_address_id ON bl_3nf.CE_STORES_SCD (STORE_ADDRESS_ID);

       
	INSERT INTO BL_3NF.CE_STORES_SCD 
	    (STORE_ID,
	    STORE_SRC_ID,
	    SOURCE_SYSTEM,
	    SOURCE_TABLE,
	    STORE_ADDRESS_ID,
	    STORE_SIZE_ID,
	    START_DT,
	    END_DT,
	    IS_ACTIVE,
	    INSERT_DT)
	SELECT
	    -1 AS STORE_ID,
	    'n.a' AS STORE_SRC_ID,
	    'MANUAL' AS SOURCE_SYSTEM,
	    'MANUAL' AS SOURCE_TABLE,
	    -1 AS STORE_ADDRESS_ID,
	    -1 AS STORE_SIZE_ID,
	    '1900-01-01' AS START_DT,
	    '9999-12-31' AS END_DT,
	    'Y' AS IS_ACTIVE,
	    '1900-01-01' AS INSERT_DT
	WHERE NOT EXISTS (
	    SELECT 1
	    FROM BL_3NF.CE_STORES_SCD
	    WHERE STORE_ID = -1
	);           
    GET DIAGNOSTICS num_rows_affected = ROW_COUNT; 
    log_message := 'Procedure CREATE_TABLE_CE_STORES_SCD completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, num_rows_affected, log_message) VALUES ('CREATE_TABLE_CE_STORES_SCD', num_rows_affected, log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in CREATE_TABLE_CE_STORES_SCD: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('CREATE_TABLE_CE_STORES_SCD', log_message);
        RAISE;
END;
$$;


CREATE OR REPLACE PROCEDURE CREATE_TABLE_CE_SALES()
LANGUAGE plpgsql
AS $$
DECLARE
    num_rows_affected integer; log_message TEXT;
BEGIN
    CREATE TABLE IF NOT EXISTS BL_3NF.CE_SALES (
        SALE_ID BIGINT,
        SALES_CHANNEL_ID BIGINT REFERENCES BL_3NF.CE_SALES_CHANNELS(SALES_CHANNEL_ID),
        STORE_ID BIGINT,
        ITEM_ID BIGINT REFERENCES BL_3NF.CE_ITEMS(ITEM_ID),
        ORDERING_WAY_ID BIGINT REFERENCES BL_3NF.CE_ORDERING_WAYS(ORDERING_WAY_ID),
        ORDER_PRIORITY_ID BIGINT REFERENCES BL_3NF.CE_ORDER_PRIORITIES(ORDER_PRIORITY_ID),
        ORDER_DATE DATE,
        PAYMENT_METHOD_ID BIGINT REFERENCES BL_3NF.CE_PAYMENT_METHODS(PAYMENT_METHOD_ID),
        CUSTOMER_TYPE_ID BIGINT REFERENCES BL_3NF.CE_CUSTOMER_TYPES(CUSTOMER_TYPE_ID),
        PROMOTION_TYPE_ID BIGINT REFERENCES BL_3NF.CE_PROMOTION_TYPES(PROMOTION_TYPE_ID),
        SALE_PERCENTAGE DECIMAL(4, 2),
        UNITS_SOLD BIGINT,
        UNIT_PRICE DECIMAL(10, 2),
        UNIT_COST DECIMAL(10, 2),
    	PRIMARY KEY (SALE_ID, ORDER_DATE)
    )
PARTITION BY RANGE (ORDER_DATE);


    CREATE SEQUENCE IF NOT EXISTS BL_3NF.sales_seq
        INCREMENT BY 1
        MINVALUE 1
        MAXVALUE 9223372036854775807
        START 1
        CACHE 1
        NO CYCLE;
    log_message := 'Procedure CREATE_TABLE_CE_SALES completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('CREATE_TABLE_CE_SALES', log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in CREATE_TABLE_CE_SALES: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('CREATE_TABLE_CE_SALES', log_message);
        RAISE;
END;
$$;


CREATE OR REPLACE PROCEDURE CREATE_PARTITIONS_CE_SALES()
LANGUAGE plpgsql
AS $$
DECLARE
    log_message TEXT;
    start_date date := '2022-01-01';
    end_date date := '2023-12-31';
    c_date date := start_date;
BEGIN
    WHILE c_date <= end_date LOOP
        IF NOT EXISTS (
            SELECT * FROM information_schema.tables 
            WHERE table_name = format('ce_sales_%s', to_char(c_date, 'YYYY_MM'))
        ) THEN
            EXECUTE format('CREATE TABLE BL_3NF.ce_sales_%s PARTITION OF BL_3NF.ce_sales FOR VALUES FROM (%L) TO (%L)',
                to_char(c_date, 'YYYY_MM'),
                c_date,
                c_date + INTERVAL '1 month'
            );
        END IF;
        c_date := c_date + INTERVAL '1 month';
    END LOOP;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = 'ce_sales_default'
    ) THEN
        EXECUTE 'CREATE TABLE BL_3NF.ce_sales_default PARTITION OF BL_3NF.ce_sales DEFAULT';
    END IF;

    log_message := 'Procedure create_partitions_ce_sales completed successfully.';
    INSERT INTO SA_SALES.log_table (procedure_name, log_message)
    VALUES ('create_partitions_ce_sales', log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in create_partitions_ce_sales: ' || SQLERRM;
        INSERT INTO SA_SALES.log_table (procedure_name, log_message)
        VALUES ('create_partitions_ce_sales', log_message);
        RAISE;
END;
$$;





CREATE OR REPLACE PROCEDURE CREATE_ALL_BL_3NF_TABLES()
LANGUAGE plpgsql
AS $$
BEGIN
	CALL BL_3NF.CREATE_TABLE_CE_ORDER_PRIORITIES();
	CALL BL_3NF.CREATE_TABLE_CE_PROMOTION_TYPES();
	CALL BL_3NF.CREATE_TABLE_CE_CUSTOMER_TYPES();
	CALL BL_3NF.CREATE_TABLE_CE_PAYMENT_METHODS();
	CALL BL_3NF.CREATE_TABLE_CE_SALES_CHANNELS();
	CALL BL_3NF.CREATE_TABLE_CE_ORDERING_WAYS();
	CALL BL_3NF.CREATE_TABLE_CE_ITEM_CATEGORIES();
	CALL BL_3NF.CREATE_TABLE_CE_ITEM_TYPES();
	CALL BL_3NF.CREATE_TABLE_CE_ITEMS();
	CALL BL_3NF.CREATE_TABLE_CE_REGIONS();
	CALL BL_3NF.CREATE_TABLE_CE_COUNTRIES();
	CALL BL_3NF.CREATE_TABLE_CE_CITIES();
	CALL BL_3NF.CREATE_TABLE_CE_ADDRESSES();
	CALL BL_3NF.CREATE_TABLE_CE_STORE_SIZES();
	CALL BL_3NF.CREATE_TABLE_CE_STORES_SCD();
	CALL BL_3NF.CREATE_TABLE_CE_SALES();
	CALL BL_3NF.CREATE_PARTITIONS_CE_SALES();

    INSERT INTO sa_sales.log_table (procedure_name, log_message)
    VALUES ('CREATE_ALL_BL_3NF_TABLES', 'All procedures completed successfully.');
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error in CREATE_ALL_BL_3NF_TABLES: %', SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message)
        VALUES ('CREATE_ALL_BL_3NF_TABLES', 'Error: ' || SQLERRM);
        RAISE;
END;
$$;


CALL CREATE_ALL_BL_3NF_TABLES();



COMMIT;
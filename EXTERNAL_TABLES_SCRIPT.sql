CREATE SCHEMA IF NOT EXISTS sa_sales;

SET search_path = sa_sales;

CREATE EXTENSION IF NOT EXISTS file_fdw;

CREATE SERVER IF NOT EXISTS pgfile FOREIGN DATA WRAPPER file_fdw;

CREATE OR REPLACE PROCEDURE create_log_table()
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'sa_sales' AND table_name = 'log_table') THEN
	CREATE TABLE IF NOT EXISTS log_table (
	    log_id SERIAL PRIMARY KEY,
	    log_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	    procedure_name TEXT,
	    num_rows_affected INT,
	    log_message TEXT
	);
	END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'An error occurred while creating the log table: %', SQLERRM;
END;
$$;

CREATE OR REPLACE PROCEDURE create_foreign_table_ext_offline_sales()
LANGUAGE plpgsql
AS $$
DECLARE
    log_message TEXT;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'sa_sales' AND table_name = 'ext_offline_sales') THEN
		CREATE FOREIGN TABLE sa_sales.ext_offline_sales (
		    sales_channel VARCHAR(255),
		    store_region VARCHAR(255),
		    store_country VARCHAR(255),
		    store_city VARCHAR(255),
		    store_address VARCHAR(255),
		    store_size VARCHAR(255),
		    store_size_description VARCHAR(255),
		    item_category VARCHAR(255),
		    item_type VARCHAR(255),
		    item VARCHAR(255),
		    order_id BIGINT,
		    order_priority VARCHAR(255),
		    order_priority_description VARCHAR(255),
		    order_date DATE,
		    payment_method VARCHAR(255),
		    payment_method_description VARCHAR(255),
		    customer_type VARCHAR(255),
		    customer_type_description VARCHAR(255),
		    promotion_type VARCHAR(255),
		    sale_percentage DECIMAL(4,2),
		    units_sold INT,
		    unit_price DECIMAL(10,2),
		    unit_cost DECIMAL(10,2),
		    total_price DECIMAL(12,2),
		    total_revenue DECIMAL(12,2)
		) SERVER pgfile
		OPTIONS ( filename 'G:\IT\EPAM DAESP23\LAB2\p2 task9\offline_sales1.csv', format 'csv', header 'true');
    END IF;
    log_message := 'Procedure create_foreign_table_ext_offline_sales completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('create_foreign_table_ext_offline_sales', log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in create_foreign_table_ext_offline_sales: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('create_foreign_table_ext_offline_sales', log_message);
        RAISE;
END;
$$;


CREATE OR REPLACE PROCEDURE create_table_src_offline_sales()
LANGUAGE plpgsql
AS $$
DECLARE
    log_message TEXT;
BEGIN
		CREATE TABLE IF NOT EXISTS sa_sales.src_offline_sales (
		    SALES_CHANNEL VARCHAR(255),
		    STORE_REGION VARCHAR(255),
		    STORE_COUNTRY VARCHAR(255),
		    STORE_CITY VARCHAR(255),
		    STORE_ADDRESS VARCHAR(255),
		    STORE_SIZE VARCHAR(255),
		    STORE_SIZE_DESCRIPTION VARCHAR(255),
		    ITEM_CATEGORY VARCHAR(255),
		    ITEM_TYPE VARCHAR(255),
		    ITEM VARCHAR(255),
		    ORDER_ID BIGINT,
		    ORDER_PRIORITY VARCHAR(255),
		    ORDER_PRIORITY_DESCRIPTION VARCHAR(255),
		    ORDER_DATE DATE,
		    PAYMENT_METHOD VARCHAR(255),
		    PAYMENT_METHOD_DESCRIPTION VARCHAR(255),
		    CUSTOMER_TYPE VARCHAR(255),
		    CUSTOMER_TYPE_DESCRIPTION VARCHAR(255),
		    PROMOTION_TYPE VARCHAR(255),
		    SALE_PERCENTAGE DECIMAL(4,2),
		    UNITS_SOLD INT,
		    UNIT_PRICE DECIMAL(10,2),
		    UNIT_COST DECIMAL(10,2),
		    TOTAL_PRICE DECIMAL(12,2),
		    TOTAL_REVENUE DECIMAL(12,2),
		    PRIMARY KEY (ORDER_ID, ORDER_DATE) 
		)
		PARTITION BY RANGE (ORDER_DATE);

	CREATE INDEX IF NOT EXISTS idx_offline_sales_order_id ON sa_sales.src_offline_sales (ORDER_ID);
	CREATE INDEX IF NOT EXISTS idx_offline_sales_order_date ON sa_sales.src_offline_sales (ORDER_DATE);
	CREATE INDEX IF NOT EXISTS idx_offline_sales_store_address ON sa_sales.src_offline_sales (STORE_ADDRESS);

    log_message := 'Procedure create_table_src_offline_sales completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('create_table_src_offline_sales', log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in create_table_src_offline_sales: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('create_table_src_offline_sales', log_message);
        RAISE;
END;
$$;


CREATE OR REPLACE PROCEDURE create_partitions_src_offline_sales()
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
            SELECT 1 FROM information_schema.tables 
            WHERE table_name = format('src_offline_sales_%s', to_char(c_date, 'YYYY_MM'))
        ) THEN
            EXECUTE format('CREATE TABLE sa_sales.src_offline_sales_%s PARTITION OF sa_sales.src_offline_sales FOR VALUES FROM (%L) TO (%L)',
                to_char(c_date, 'YYYY_MM'),
                c_date,
                c_date + INTERVAL '1 month'
            );
        END IF;
        c_date := c_date + INTERVAL '1 month';
    END LOOP;
	
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = 'src_offline_sales_default'
    ) THEN
        EXECUTE 'CREATE TABLE sa_sales.src_offline_sales_default PARTITION OF sa_sales.src_offline_sales DEFAULT';
    END IF;
    log_message := 'Procedure create_partitions_src_offline_sales completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('create_partitions_src_offline_sales', log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in create_partitions_src_offline_sales: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('create_partitions_src_offline_sales', log_message);
        RAISE;
END;
$$;



CREATE OR REPLACE PROCEDURE insert_data_into_src_offline_sales()
LANGUAGE plpgsql
AS $$
DECLARE
    num_rows_affected integer; log_message TEXT;
BEGIN
	INSERT INTO sa_sales.src_offline_sales (
	    SALES_CHANNEL,
	    STORE_REGION,
	    STORE_COUNTRY,
	    STORE_CITY,
	    STORE_ADDRESS,
	    STORE_SIZE,
	    STORE_SIZE_DESCRIPTION,
	    ITEM_CATEGORY,
	    ITEM_TYPE,
	    ITEM,
	    ORDER_ID,
	    ORDER_PRIORITY,
	    ORDER_PRIORITY_DESCRIPTION,
	    ORDER_DATE,
	    PAYMENT_METHOD,
	    PAYMENT_METHOD_DESCRIPTION,
	    CUSTOMER_TYPE,
	    CUSTOMER_TYPE_DESCRIPTION,
	    PROMOTION_TYPE,
	    SALE_PERCENTAGE,
	    UNITS_SOLD,
	    UNIT_PRICE,
	    UNIT_COST,
	    TOTAL_PRICE,
	    TOTAL_REVENUE
	)
	SELECT
	    sales_channel,
	    store_region,
	    store_country,
	    store_city,
	    store_address,
	    store_size,
	    store_size_description,
	    item_category,
	    item_type,
	    item,
	    order_id,
	    order_priority,
	    order_priority_description,
	    order_date,
	    payment_method,
	    payment_method_description,
	    customer_type,
	    customer_type_description,
	    promotion_type,
	    sale_percentage,
	    units_sold,
	    unit_price,
	    unit_cost,
	    total_price,
	    total_revenue
	FROM sa_sales.ext_offline_sales eos
	WHERE NOT EXISTS (
	    SELECT 1
	    FROM sa_sales.src_offline_sales sos
	    WHERE eos.order_id = sos.order_id
	    AND eos.order_date = sos.order_date
	);
    GET DIAGNOSTICS num_rows_affected = ROW_COUNT; 
    log_message := 'Procedure insert_data_into_src_offline_sales completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, num_rows_affected, log_message) VALUES ('insert_data_into_src_offline_sales', num_rows_affected, log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in insert_data_into_src_offline_sales: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('insert_data_into_src_offline_sales', log_message);
        RAISE;
END;
$$;



CREATE OR REPLACE PROCEDURE create_foreign_table_ext_online_sales()
LANGUAGE plpgsql
AS $$
DECLARE
    log_message TEXT;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'sa_sales' AND table_name = 'ext_online_sales') THEN
        CREATE FOREIGN TABLE sa_sales.ext_online_sales (
            sales_channel VARCHAR(255),
            store_region VARCHAR(255),
            store_country VARCHAR(255),
            store_city VARCHAR(255),
            store_address VARCHAR(255),
            item_category VARCHAR(255),
            item_type VARCHAR(255),
            item VARCHAR(255),
            order_id BIGINT,
            ordering_way VARCHAR(255),
            ordering_way_description VARCHAR(255),
            order_priority VARCHAR(255),
            order_priority_description VARCHAR(255),
            order_date DATE,
            payment_method VARCHAR(255),
            payment_method_description VARCHAR(255),
            customer_type VARCHAR(255),
            customer_type_description VARCHAR(255),
            promotion_type VARCHAR(255),
            sale_percentage DECIMAL(4, 2),
            units_sold INT,
            unit_price DECIMAL(10, 2),
            unit_cost DECIMAL(10, 2),
            total_price DECIMAL(12, 2),
            total_revenue DECIMAL(12, 2)
        ) SERVER pgfile
        OPTIONS (filename 'G:\IT\EPAM DAESP23\LAB2\p2 task9\online_sales1.csv', format 'csv', header 'true');
    END IF;
    log_message := 'Procedure create_foreign_table_ext_online_sales completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('create_foreign_table_ext_online_sales', log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in create_foreign_table_ext_online_sales: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('create_foreign_table_ext_online_sales', log_message);
        RAISE;
END;
$$;









CREATE OR REPLACE PROCEDURE create_table_src_online_sales()
LANGUAGE plpgsql
AS $$
DECLARE
    log_message TEXT;
BEGIN
        CREATE TABLE IF NOT EXISTS sa_sales.src_online_sales (
            SALES_CHANNEL VARCHAR(255),
            STORE_REGION VARCHAR(255),
            STORE_COUNTRY VARCHAR(255),
            STORE_CITY VARCHAR(255),
            STORE_ADDRESS VARCHAR(255),
            ITEM_CATEGORY VARCHAR(255),
            ITEM_TYPE VARCHAR(255),
            ITEM VARCHAR(255),
            ORDER_ID BIGINT,
            ORDERING_WAY VARCHAR(255),
            ORDERING_WAY_DESCRIPTION VARCHAR(255),
            ORDER_PRIORITY VARCHAR(255),
            ORDER_PRIORITY_DESCRIPTION VARCHAR(255),
            ORDER_DATE date,
            PAYMENT_METHOD VARCHAR(255),
            PAYMENT_METHOD_DESCRIPTION VARCHAR(255),
            CUSTOMER_TYPE VARCHAR(255),
            CUSTOMER_TYPE_DESCRIPTION VARCHAR(255),
            PROMOTION_TYPE VARCHAR(255),
            SALE_PERCENTAGE DECIMAL(4, 2),
            UNITS_SOLD INT,
            UNIT_PRICE DECIMAL(10, 2),
            UNIT_COST DECIMAL(10, 2),
            TOTAL_PRICE DECIMAL(12, 2),
            TOTAL_REVENUE DECIMAL(12, 2),
            PRIMARY KEY (ORDER_ID, ORDER_DATE)
        ) PARTITION BY RANGE (ORDER_DATE);

    CREATE INDEX IF NOT EXISTS idx_online_sales_order_id ON sa_sales.src_online_sales (ORDER_ID);
	CREATE INDEX IF NOT EXISTS idx_online_sales_store_address ON sa_sales.src_online_sales (STORE_ADDRESS);
	CREATE INDEX IF NOT EXISTS idx_online_sales_order_date ON sa_sales.src_online_sales (ORDER_DATE);

    log_message := 'Procedure create_table_src_online_sales completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('create_table_src_online_sales', log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in create_table_src_online_sales: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('create_table_src_online_sales', log_message);
        RAISE;
END;
$$;

CREATE OR REPLACE PROCEDURE create_partitions_src_online_sales()
LANGUAGE plpgsql
AS $$
DECLARE
    num_rows_affected integer; log_message TEXT;
    start_date date := '2022-01-01';
    end_date date := '2023-12-31';
    c_date date := start_date;
BEGIN
    WHILE c_date <= end_date LOOP
        IF NOT EXISTS (
            SELECT * FROM information_schema.tables 
            WHERE table_name = format('src_online_sales_%s', to_char(c_date, 'YYYY_MM'))
        ) THEN
            EXECUTE format('CREATE TABLE sa_sales.src_online_sales_%s PARTITION OF sa_sales.src_online_sales FOR VALUES FROM (%L) TO (%L)',
                to_char(c_date, 'YYYY_MM'),
                c_date,
                c_date + INTERVAL '1 month'
            );
        END IF;
        c_date := c_date + INTERVAL '1 month';
    END LOOP;
    
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = 'src_online_sales_default'
    ) THEN
        EXECUTE 'CREATE TABLE sa_sales.src_online_sales_default PARTITION OF sa_sales.src_online_sales DEFAULT';
    END IF;
    log_message := 'Procedure create_partitions_src_online_sales completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('create_partitions_src_online_sales', log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in create_partitions_src_online_sales: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('create_partitions_src_online_sales', log_message);
        RAISE;
END;
$$;



CREATE OR REPLACE PROCEDURE insert_data_into_src_online_sales()
LANGUAGE plpgsql
AS $$
DECLARE
    num_rows_affected integer; log_message TEXT;
BEGIN
    INSERT INTO sa_sales.src_online_sales (
        SALES_CHANNEL,
        STORE_REGION,
        STORE_COUNTRY,
        STORE_CITY,
        STORE_ADDRESS,
        ITEM_CATEGORY,
        ITEM_TYPE,
        ITEM,
        ORDER_ID,
        ORDERING_WAY,
        ORDERING_WAY_DESCRIPTION,
        ORDER_PRIORITY,
        ORDER_PRIORITY_DESCRIPTION,
        ORDER_DATE,
        PAYMENT_METHOD,
        PAYMENT_METHOD_DESCRIPTION,
        CUSTOMER_TYPE,
        CUSTOMER_TYPE_DESCRIPTION,
        PROMOTION_TYPE,
        SALE_PERCENTAGE,
        UNITS_SOLD,
        UNIT_PRICE,
        UNIT_COST,
        TOTAL_PRICE,
        TOTAL_REVENUE
    )
    SELECT
        sales_channel,
        store_region,
        store_country,
        store_city,
        store_address,
        item_category,
        item_type,
        item,
        order_id,
        ordering_way,
        ordering_way_description,
        order_priority,
        order_priority_description,
        order_date,
        payment_method,
        payment_method_description,
        customer_type,
        customer_type_description,
        promotion_type,
        sale_percentage,
        units_sold,
        unit_price,
        unit_cost,
        total_price,
        total_revenue
    FROM sa_sales.ext_online_sales eos
    WHERE NOT EXISTS (
        SELECT 1
        FROM sa_sales.src_online_sales sos
        WHERE eos.order_id = sos.order_id
        AND eos.order_date = sos.order_date
    );
    GET DIAGNOSTICS num_rows_affected = ROW_COUNT; 
    log_message := 'Procedure insert_data_into_src_online_sales completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, num_rows_affected, log_message) VALUES ('insert_data_into_src_online_sales', num_rows_affected, log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in insert_data_into_src_online_sales: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('insert_data_into_src_online_sales', log_message);
        RAISE;
END;
$$;



CREATE OR REPLACE PROCEDURE LOAD_FROM_FILES()
LANGUAGE plpgsql
AS $$
BEGIN

	CALL SA_SALES.create_log_table();
	
	CALL SA_SALES.create_foreign_table_ext_offline_sales();
	CALL SA_SALES.create_table_src_offline_sales();
	CALL SA_SALES.create_partitions_src_offline_sales();
	CALL SA_SALES.insert_data_into_src_offline_sales();
	
	CALL SA_SALES.create_foreign_table_ext_online_sales();
	CALL SA_SALES.create_table_src_online_sales();
	CALL SA_SALES.create_partitions_src_online_sales();
	CALL SA_SALES.insert_data_into_src_online_sales();

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error in LOAD_FROM_FILES: %', SQLERRM;
END;
$$;


CALL LOAD_FROM_FILES();

COMMIT;

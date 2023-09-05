SET search_path = bl_dm;



CREATE OR REPLACE PROCEDURE BL_DM.LD_DIM_CUSTOMER_TYPES()
LANGUAGE plpgsql
AS $$
DECLARE
    num_rows_affected integer;
    log_message TEXT;
BEGIN
    WITH cte AS (
        SELECT
            CUSTOMER_TYPE_ID AS CUSTOMER_TYPE_SRC_ID,
            'BL_3NF' AS SOURCE_SYSTEM,
            'CE_CUSTOMER_TYPES' AS SOURCE_TABLE,
            CUSTOMER_TYPE_NAME,
            CUSTOMER_TYPE_DESCRIPTION
        FROM BL_3NF.CE_CUSTOMER_TYPES
        WHERE CUSTOMER_TYPE_ID <> -1
    )
    INSERT INTO BL_DM.DIM_CUSTOMER_TYPES
        (CUSTOMER_TYPE_SURR_ID,
        CUSTOMER_TYPE_SRC_ID,
        SOURCE_SYSTEM,
        SOURCE_TABLE,
        CUSTOMER_TYPE_NAME,
        CUSTOMER_TYPE_DESCRIPTION,
        INSERT_DT,
        UPDATE_DT)
    SELECT
        nextval('bl_dm.dim_CUSTOMER_TYPES_SEQ') AS CUSTOMER_TYPE_SURR_ID,
        COALESCE(cte.CUSTOMER_TYPE_SRC_ID::text, (SELECT CUSTOMER_TYPE_SRC_ID FROM BL_DM.DIM_CUSTOMER_TYPES ct WHERE ct.CUSTOMER_TYPE_SURR_ID = -1)) AS CUSTOMER_TYPE_SRC_ID,
        COALESCE(cte.SOURCE_SYSTEM, (SELECT SOURCE_SYSTEM FROM BL_DM.DIM_CUSTOMER_TYPES ct WHERE ct.CUSTOMER_TYPE_SURR_ID = -1)) AS SOURCE_SYSTEM,
        COALESCE(cte.SOURCE_TABLE, (SELECT SOURCE_TABLE FROM BL_DM.DIM_CUSTOMER_TYPES ct WHERE ct.CUSTOMER_TYPE_SURR_ID = -1)) AS SOURCE_TABLE,
        COALESCE(UPPER(cte.CUSTOMER_TYPE_NAME), (SELECT CUSTOMER_TYPE_NAME FROM BL_DM.DIM_CUSTOMER_TYPES ct WHERE ct.CUSTOMER_TYPE_SURR_ID = -1)) AS CUSTOMER_TYPE_NAME,
        COALESCE(cte.CUSTOMER_TYPE_DESCRIPTION, (SELECT CUSTOMER_TYPE_DESCRIPTION FROM BL_DM.DIM_CUSTOMER_TYPES ct WHERE ct.CUSTOMER_TYPE_SURR_ID = -1)) AS CUSTOMER_TYPE_DESCRIPTION,
        COALESCE(CURRENT_DATE, (SELECT INSERT_DT FROM BL_DM.DIM_CUSTOMER_TYPES ct WHERE ct.CUSTOMER_TYPE_SURR_ID = -1)) AS INSERT_DT,
        COALESCE(CURRENT_DATE, (SELECT UPDATE_DT FROM BL_DM.DIM_CUSTOMER_TYPES ct WHERE ct.CUSTOMER_TYPE_SURR_ID = -1)) AS UPDATE_DT
    FROM cte
    WHERE NOT EXISTS (
        SELECT 1
        FROM BL_DM.DIM_CUSTOMER_TYPES ct
        WHERE UPPER(cte.CUSTOMER_TYPE_NAME) = UPPER(ct.CUSTOMER_TYPE_NAME)
        AND UPPER(ct.CUSTOMER_TYPE_SRC_ID) = UPPER(cte.CUSTOMER_TYPE_SRC_ID::text)
        AND UPPER(ct.SOURCE_SYSTEM) = UPPER(cte.SOURCE_SYSTEM)
        AND UPPER(ct.SOURCE_TABLE) = UPPER(cte.SOURCE_TABLE)
    );

    GET DIAGNOSTICS num_rows_affected = ROW_COUNT;
    log_message := 'Procedure LD_DIM_CUSTOMER_TYPES completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, num_rows_affected, log_message) VALUES ('LD_DIM_CUSTOMER_TYPES', num_rows_affected, log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in LD_DIM_CUSTOMER_TYPES: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('LD_DIM_CUSTOMER_TYPES', log_message);
        RAISE;
END;
$$;



CREATE OR REPLACE PROCEDURE BL_DM.LD_DIM_PAYMENT_METHODS()
LANGUAGE plpgsql
AS $$
DECLARE
    num_rows_affected integer;
    log_message TEXT;
BEGIN
    CREATE TYPE payment_method_info AS (
        payment_method_id bigint,
        payment_method_name VARCHAR,
        payment_method_description VARCHAR
    );
    WITH cte AS (
        SELECT 
            'BL_3NF' AS SOURCE_SYSTEM,
            'CE_PAYMENT_METHODS' AS SOURCE_TABLE,
            ROW(PAYMENT_METHOD_ID,
                PAYMENT_METHOD_NAME,
                PAYMENT_METHOD_DESCRIPTION)::payment_method_info AS pmi
        FROM BL_3NF.CE_PAYMENT_METHODS
        WHERE PAYMENT_METHOD_ID <> -1
    )
    INSERT INTO BL_DM.DIM_PAYMENT_METHODS
        (PAYMENT_METHOD_SURR_ID,
        PAYMENT_METHOD_SRC_ID,
        SOURCE_SYSTEM,
        SOURCE_TABLE,
        PAYMENT_METHOD_NAME,
        PAYMENT_METHOD_DESCRIPTION,
        INSERT_DT,
        UPDATE_DT)
    SELECT
        nextval('bl_dm.dim_PAYMENT_METHODS_SEQ') AS PAYMENT_METHOD_SURR_ID,
        COALESCE((cte).pmi.PAYMENT_METHOD_ID::text, (SELECT PAYMENT_METHOD_SRC_ID FROM BL_DM.DIM_PAYMENT_METHODS pm WHERE pm.PAYMENT_METHOD_SURR_ID = -1)) AS PAYMENT_METHOD_SRC_ID,
        COALESCE(cte.SOURCE_SYSTEM, (SELECT SOURCE_SYSTEM FROM BL_DM.DIM_PAYMENT_METHODS pm WHERE pm.PAYMENT_METHOD_SURR_ID = -1)) AS SOURCE_SYSTEM,
        COALESCE(cte.SOURCE_TABLE, (SELECT SOURCE_TABLE FROM BL_DM.DIM_PAYMENT_METHODS pm WHERE pm.PAYMENT_METHOD_SURR_ID = -1)) AS SOURCE_TABLE,
        COALESCE(UPPER((cte).pmi.PAYMENT_METHOD_NAME), (SELECT PAYMENT_METHOD_NAME FROM BL_DM.DIM_PAYMENT_METHODS pm WHERE pm.PAYMENT_METHOD_SURR_ID = -1)) AS PAYMENT_METHOD_NAME,
        COALESCE((cte).pmi.PAYMENT_METHOD_DESCRIPTION, (SELECT PAYMENT_METHOD_DESCRIPTION FROM BL_DM.DIM_PAYMENT_METHODS pm WHERE pm.PAYMENT_METHOD_SURR_ID = -1)) AS PAYMENT_METHOD_DESCRIPTION,
        COALESCE(CURRENT_DATE, (SELECT INSERT_DT FROM BL_DM.DIM_PAYMENT_METHODS pm WHERE pm.PAYMENT_METHOD_SURR_ID = -1)) AS INSERT_DT,
        COALESCE(CURRENT_DATE, (SELECT UPDATE_DT FROM BL_DM.DIM_PAYMENT_METHODS pm WHERE pm.PAYMENT_METHOD_SURR_ID = -1)) AS UPDATE_DT
    FROM cte
    WHERE NOT EXISTS (
        SELECT 1
        FROM BL_DM.DIM_PAYMENT_METHODS pm
        WHERE UPPER((cte).pmi.PAYMENT_METHOD_NAME) = UPPER(pm.PAYMENT_METHOD_NAME)
        AND UPPER(pm.PAYMENT_METHOD_SRC_ID) = UPPER((cte).pmi.PAYMENT_METHOD_ID::text)
        AND UPPER(pm.SOURCE_SYSTEM) = UPPER(cte.SOURCE_SYSTEM)
        AND UPPER(pm.SOURCE_TABLE) = UPPER(cte.SOURCE_TABLE)
    );

    GET DIAGNOSTICS num_rows_affected = ROW_COUNT;
    log_message := 'Procedure LD_DIM_PAYMENT_METHODS completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, num_rows_affected, log_message) VALUES ('LD_DIM_PAYMENT_METHODS', num_rows_affected, log_message);

    DROP TYPE payment_method_info;
EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in LD_DIM_PAYMENT_METHODS: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('LD_DIM_PAYMENT_METHODS', log_message);
        RAISE;
END;
$$;



CREATE OR REPLACE PROCEDURE BL_DM.LD_DIM_ORDERING_WAYS()
LANGUAGE plpgsql
AS $$
DECLARE
    num_rows_affected integer;
    log_message TEXT;
BEGIN
    WITH cte AS (
        SELECT 
            ORDERING_WAY_ID AS ORDERING_WAY_SRC_ID,
            'BL_3NF' AS SOURCE_SYSTEM,
            'CE_ORDERING_WAYS' AS SOURCE_TABLE,
            ORDERING_WAY_NAME,
            ORDERING_WAY_DESCRIPTION
        FROM BL_3NF.CE_ORDERING_WAYS
        WHERE ORDERING_WAY_ID <> -1
    )
    INSERT INTO BL_DM.DIM_ORDERING_WAYS
        (ORDERING_WAY_SURR_ID,
        ORDERING_WAY_SRC_ID,
        SOURCE_SYSTEM,
        SOURCE_TABLE,
        ORDERING_WAY_NAME,
        ORDERING_WAY_DESCRIPTION,
        INSERT_DT,
        UPDATE_DT)
    SELECT
        nextval('bl_dm.dim_ORDERING_WAYS_SEQ') AS ORDERING_WAY_SURR_ID,
        COALESCE(cte.ORDERING_WAY_SRC_ID::text, (SELECT ORDERING_WAY_SRC_ID FROM BL_DM.DIM_ORDERING_WAYS ow WHERE ow.ORDERING_WAY_SURR_ID = -1)) AS ORDERING_WAY_SRC_ID,
        COALESCE(cte.SOURCE_SYSTEM, (SELECT SOURCE_SYSTEM FROM BL_DM.DIM_ORDERING_WAYS ow WHERE ow.ORDERING_WAY_SURR_ID = -1)) AS SOURCE_SYSTEM,
        COALESCE(cte.SOURCE_TABLE, (SELECT SOURCE_TABLE FROM BL_DM.DIM_ORDERING_WAYS ow WHERE ow.ORDERING_WAY_SURR_ID = -1)) AS SOURCE_TABLE,
        COALESCE(UPPER(cte.ORDERING_WAY_NAME), (SELECT ORDERING_WAY_NAME FROM BL_DM.DIM_ORDERING_WAYS ow WHERE ow.ORDERING_WAY_SURR_ID = -1)) AS ORDERING_WAY_NAME,
        COALESCE(cte.ORDERING_WAY_DESCRIPTION, (SELECT ORDERING_WAY_DESCRIPTION FROM BL_DM.DIM_ORDERING_WAYS ow WHERE ow.ORDERING_WAY_SURR_ID = -1)) AS ORDERING_WAY_DESCRIPTION,
        COALESCE(CURRENT_DATE, (SELECT INSERT_DT FROM BL_DM.DIM_ORDERING_WAYS ow WHERE ow.ORDERING_WAY_SURR_ID = -1)) AS INSERT_DT,
        COALESCE(CURRENT_DATE, (SELECT UPDATE_DT FROM BL_DM.DIM_ORDERING_WAYS ow WHERE ow.ORDERING_WAY_SURR_ID = -1)) AS UPDATE_DT
    FROM cte
    WHERE NOT EXISTS (
        SELECT 1
        FROM BL_DM.DIM_ORDERING_WAYS ow
        WHERE UPPER(cte.ORDERING_WAY_NAME) = UPPER(ow.ORDERING_WAY_NAME)
        AND UPPER(ow.ORDERING_WAY_SRC_ID) = UPPER(cte.ORDERING_WAY_SRC_ID::text)
        AND UPPER(ow.SOURCE_SYSTEM) = UPPER(cte.SOURCE_SYSTEM)
        AND UPPER(ow.SOURCE_TABLE) = UPPER(cte.SOURCE_TABLE)
    );

    GET DIAGNOSTICS num_rows_affected = ROW_COUNT;
    log_message := 'Procedure LD_DIM_ORDERING_WAYS completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, num_rows_affected, log_message) VALUES ('LD_DIM_ORDERING_WAYS', num_rows_affected, log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in LD_DIM_ORDERING_WAYS: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('LD_DIM_ORDERING_WAYS', log_message);
        RAISE;
END;
$$;



CREATE OR REPLACE PROCEDURE BL_DM.LD_DIM_SALES_CHANNELS()
LANGUAGE plpgsql
AS $$
DECLARE
    num_rows_affected integer;
    log_message TEXT;
BEGIN
    WITH cte AS (
        SELECT 
            SALES_CHANNEL_ID AS SALES_CHANNEL_SRC_ID,
            'BL_3NF' AS SOURCE_SYSTEM,
            'CE_SALES_CHANNELS' AS SOURCE_TABLE,
            SALES_CHANNEL_NAME
        FROM BL_3NF.CE_SALES_CHANNELS
        WHERE SALES_CHANNEL_ID <> -1
    )
    INSERT INTO BL_DM.DIM_SALES_CHANNELS
        (SALES_CHANNEL_SURR_ID,
        SALES_CHANNEL_SRC_ID,
        SOURCE_SYSTEM,
        SOURCE_TABLE,
        SALES_CHANNEL_NAME,
        INSERT_DT,
        UPDATE_DT)
    SELECT
        nextval('bl_dm.dim_SALES_CHANNELS_SEQ') AS SALES_CHANNEL_SURR_ID,
        COALESCE(cte.SALES_CHANNEL_SRC_ID::text, (SELECT SALES_CHANNEL_SRC_ID FROM BL_DM.DIM_SALES_CHANNELS sc WHERE sc.SALES_CHANNEL_SURR_ID = -1)) AS SALES_CHANNEL_SRC_ID,
        COALESCE(cte.SOURCE_SYSTEM, (SELECT SOURCE_SYSTEM FROM BL_DM.DIM_SALES_CHANNELS sc WHERE sc.SALES_CHANNEL_SURR_ID = -1)) AS SOURCE_SYSTEM,
        COALESCE(cte.SOURCE_TABLE, (SELECT SOURCE_TABLE FROM BL_DM.DIM_SALES_CHANNELS sc WHERE sc.SALES_CHANNEL_SURR_ID = -1)) AS SOURCE_TABLE,
        COALESCE(UPPER(cte.SALES_CHANNEL_NAME), (SELECT SALES_CHANNEL_NAME FROM BL_DM.DIM_SALES_CHANNELS sc WHERE sc.SALES_CHANNEL_SURR_ID = -1)) AS SALES_CHANNEL_NAME,
        COALESCE(CURRENT_DATE, (SELECT INSERT_DT FROM BL_DM.DIM_SALES_CHANNELS sc WHERE sc.SALES_CHANNEL_SURR_ID = -1)) AS INSERT_DT,
        COALESCE(CURRENT_DATE, (SELECT UPDATE_DT FROM BL_DM.DIM_SALES_CHANNELS sc WHERE sc.SALES_CHANNEL_SURR_ID = -1)) AS UPDATE_DT
    FROM cte
    WHERE NOT EXISTS (
        SELECT 1
        FROM BL_DM.DIM_SALES_CHANNELS sc
        WHERE UPPER(cte.SALES_CHANNEL_NAME) = UPPER(sc.SALES_CHANNEL_NAME)
        AND UPPER(sc.SALES_CHANNEL_SRC_ID) = UPPER(cte.SALES_CHANNEL_SRC_ID::text)
        AND UPPER(sc.SOURCE_SYSTEM) = UPPER(cte.SOURCE_SYSTEM)
        AND UPPER(sc.SOURCE_TABLE) = UPPER(cte.SOURCE_TABLE)
    );

    GET DIAGNOSTICS num_rows_affected = ROW_COUNT;
    log_message := 'Procedure LD_DIM_SALES_CHANNELS completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, num_rows_affected, log_message) VALUES ('LD_DIM_SALES_CHANNELS', num_rows_affected, log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in LD_DIM_SALES_CHANNELS: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('LD_DIM_SALES_CHANNELS', log_message);
        RAISE;
END;
$$;





CREATE OR REPLACE PROCEDURE BL_DM.LD_DIM_PROMOTION_TYPES()
LANGUAGE plpgsql
AS $$
DECLARE
    num_rows_affected integer;
    log_message TEXT;
BEGIN
    WITH cte AS (
        SELECT 
            PROMOTION_TYPE_ID AS PROMOTION_TYPE_SRC_ID,
            'BL_3NF' AS SOURCE_SYSTEM,
            'CE_PROMOTION_TYPES' AS SOURCE_TABLE,
            PROMOTION_TYPE_NAME,
            SALE_PERCENTAGE
        FROM BL_3NF.CE_PROMOTION_TYPES
        WHERE PROMOTION_TYPE_ID <> -1
    )
    INSERT INTO BL_DM.DIM_PROMOTION_TYPES
        (PROMOTION_TYPE_SURR_ID,
        PROMOTION_TYPE_SRC_ID,
        SOURCE_SYSTEM,
        SOURCE_TABLE,
        PROMOTION_TYPE_NAME,
        SALE_PERCENTAGE,
        INSERT_DT,
        UPDATE_DT)
    SELECT
        nextval('bl_dm.dim_PROMOTION_TYPES_SEQ') AS PROMOTION_TYPE_SURR_ID,
        COALESCE(cte.PROMOTION_TYPE_SRC_ID::text, (SELECT PROMOTION_TYPE_SRC_ID FROM BL_DM.DIM_PROMOTION_TYPES pt WHERE pt.PROMOTION_TYPE_SURR_ID = -1)) AS PROMOTION_TYPE_SRC_ID,
        COALESCE(cte.SOURCE_SYSTEM, (SELECT SOURCE_SYSTEM FROM BL_DM.DIM_PROMOTION_TYPES pt WHERE pt.PROMOTION_TYPE_SURR_ID = -1)) AS SOURCE_SYSTEM,
        COALESCE(cte.SOURCE_TABLE, (SELECT SOURCE_TABLE FROM BL_DM.DIM_PROMOTION_TYPES pt WHERE pt.PROMOTION_TYPE_SURR_ID = -1)) AS SOURCE_TABLE,
        COALESCE(UPPER(cte.PROMOTION_TYPE_NAME), (SELECT PROMOTION_TYPE_NAME FROM BL_DM.DIM_PROMOTION_TYPES pt WHERE pt.PROMOTION_TYPE_SURR_ID = -1)) AS PROMOTION_TYPE_NAME,
        COALESCE(cte.SALE_PERCENTAGE, (SELECT SALE_PERCENTAGE FROM BL_DM.DIM_PROMOTION_TYPES pt WHERE pt.PROMOTION_TYPE_SURR_ID = -1)) AS SALE_PERCENTAGE,
        COALESCE(CURRENT_DATE, (SELECT INSERT_DT FROM BL_DM.DIM_PROMOTION_TYPES pt WHERE pt.PROMOTION_TYPE_SURR_ID = -1)) AS INSERT_DT,
        COALESCE(CURRENT_DATE, (SELECT UPDATE_DT FROM BL_DM.DIM_PROMOTION_TYPES pt WHERE pt.PROMOTION_TYPE_SURR_ID = -1)) AS UPDATE_DT
    FROM cte
    WHERE NOT EXISTS (
        SELECT 1
        FROM BL_DM.DIM_PROMOTION_TYPES pt
        WHERE UPPER(cte.PROMOTION_TYPE_NAME) = UPPER(pt.PROMOTION_TYPE_NAME)
        AND UPPER(pt.PROMOTION_TYPE_SRC_ID) = UPPER(cte.PROMOTION_TYPE_SRC_ID::text)
        AND UPPER(pt.SOURCE_SYSTEM) = UPPER(cte.SOURCE_SYSTEM)
        AND UPPER(pt.SOURCE_TABLE) = UPPER(cte.SOURCE_TABLE)
    );

    GET DIAGNOSTICS num_rows_affected = ROW_COUNT;
    log_message := 'Procedure LD_DIM_PROMOTION_TYPES completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, num_rows_affected, log_message) VALUES ('LD_DIM_PROMOTION_TYPES', num_rows_affected, log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in LD_DIM_PROMOTION_TYPES: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('LD_DIM_PROMOTION_TYPES', log_message);
        RAISE;
END;
$$;




CREATE OR REPLACE PROCEDURE BL_DM.LD_DIM_ITEMS()
LANGUAGE plpgsql
AS $$
DECLARE
    num_rows_affected integer;
    log_message TEXT;
BEGIN
    WITH cte AS (
        SELECT 
            ITEM_ID AS ITEM_SRC_ID,
            'BL_3NF' AS SOURCE_SYSTEM,
            'CE_ITEMS <- CE_ITEM_TYPES <- CE_ITEM_CATEGORIES' AS SOURCE_TABLE,
            ci.ITEM_NAME,
            ci.ITEM_TYPE_ID,
            cit.ITEM_TYPE_NAME,
            cit.ITEM_CATEGORY_ID,
            cic.ITEM_CATEGORY_NAME
        FROM BL_3NF.CE_ITEMS ci
        LEFT JOIN BL_3NF.CE_ITEM_TYPES cit ON ci.ITEM_TYPE_ID = cit.ITEM_TYPE_ID
        LEFT JOIN BL_3NF.CE_ITEM_CATEGORIES cic ON cit.ITEM_CATEGORY_ID = cic.ITEM_CATEGORY_ID
        WHERE ITEM_ID <> -1
    )
    INSERT INTO BL_DM.DIM_ITEMS
        (ITEM_SURR_ID,
        ITEM_SRC_ID,
        SOURCE_SYSTEM,
        SOURCE_TABLE,
        ITEM_NAME,
        ITEM_TYPE_ID,
        ITEM_TYPE_NAME,
        ITEM_CATEGORY_ID,
        ITEM_CATEGORY_NAME,
        INSERT_DT,
        UPDATE_DT)
    SELECT
        nextval('bl_dm.dim_ITEMS_SEQ') AS ITEM_SURR_ID,
        COALESCE(cte.ITEM_SRC_ID::text, (SELECT ITEM_SRC_ID FROM BL_DM.DIM_ITEMS di WHERE di.ITEM_SURR_ID = -1)) AS ITEM_SRC_ID,
        COALESCE(cte.SOURCE_SYSTEM, (SELECT SOURCE_SYSTEM FROM BL_DM.DIM_ITEMS di WHERE di.ITEM_SURR_ID = -1)) AS SOURCE_SYSTEM,
        COALESCE(cte.SOURCE_TABLE, (SELECT SOURCE_TABLE FROM BL_DM.DIM_ITEMS di WHERE di.ITEM_SURR_ID = -1)) AS SOURCE_TABLE,
        COALESCE(UPPER(cte.ITEM_NAME), (SELECT ITEM_NAME FROM BL_DM.DIM_ITEMS di WHERE di.ITEM_SURR_ID = -1)) AS ITEM_NAME,
        COALESCE(cte.ITEM_TYPE_ID, (SELECT ITEM_TYPE_ID FROM BL_DM.DIM_ITEMS di WHERE di.ITEM_SURR_ID = -1)) AS ITEM_TYPE_ID,  
        COALESCE(UPPER(cte.ITEM_TYPE_NAME), (SELECT ITEM_TYPE_NAME FROM BL_DM.DIM_ITEMS di WHERE di.ITEM_SURR_ID = -1)) AS ITEM_TYPE_NAME,
        COALESCE(cte.ITEM_CATEGORY_ID, (SELECT ITEM_CATEGORY_ID FROM BL_DM.DIM_ITEMS di WHERE di.ITEM_SURR_ID = -1)) AS ITEM_CATEGORY_ID,  
        COALESCE(UPPER(cte.ITEM_CATEGORY_NAME), (SELECT ITEM_CATEGORY_NAME FROM BL_DM.DIM_ITEMS di WHERE di.ITEM_SURR_ID = -1)) AS ITEM_CATEGORY_NAME,
        COALESCE(CURRENT_DATE, (SELECT INSERT_DT FROM BL_DM.DIM_ITEMS di WHERE di.ITEM_SURR_ID = -1)) AS INSERT_DT,
        COALESCE(CURRENT_DATE, (SELECT UPDATE_DT FROM BL_DM.DIM_ITEMS di WHERE di.ITEM_SURR_ID = -1)) AS UPDATE_DT
    FROM cte
    WHERE NOT EXISTS (
        SELECT 1
        FROM BL_DM.DIM_ITEMS di
        WHERE UPPER(cte.ITEM_NAME) = UPPER(di.ITEM_NAME)
        AND UPPER(di.ITEM_SRC_ID) = UPPER(cte.ITEM_SRC_ID::text)
        AND UPPER(di.SOURCE_SYSTEM) = UPPER(cte.SOURCE_SYSTEM)
        AND UPPER(di.SOURCE_TABLE) = UPPER(cte.SOURCE_TABLE)
    );

    GET DIAGNOSTICS num_rows_affected = ROW_COUNT;
    log_message := 'Procedure LD_DIM_ITEMS completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, num_rows_affected, log_message) VALUES ('LD_DIM_ITEMS', num_rows_affected, log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in LD_DIM_ITEMS: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('LD_DIM_ITEMS', log_message);
        RAISE;
END;
$$;




CREATE OR REPLACE PROCEDURE BL_DM.LD_DIM_ORDER_PRIORITIES()
LANGUAGE plpgsql
AS $$
DECLARE
    num_rows_affected integer;
    log_message TEXT;
BEGIN
    WITH cte AS (
	SELECT 
	    ORDER_PRIORITY_ID AS ORDER_PRIORITY_SRC_ID,
	    'BL_3NF' AS SOURCE_SYSTEM,
	    'CE_ORDER_PRIORITIES' AS SOURCE_TABLE,
	    ORDER_PRIORITY_NAME,
	    ORDER_PRIORITY_DESCRIPTION
		FROM BL_3NF.CE_ORDER_PRIORITIES
        WHERE ORDER_PRIORITY_ID <> -1
    )
    INSERT INTO BL_DM.DIM_ORDER_PRIORITIES
        (ORDER_PRIORITY_SURR_ID,
        ORDER_PRIORITY_SRC_ID,
        SOURCE_SYSTEM,
        SOURCE_TABLE,
        ORDER_PRIORITY_NAME,
        ORDER_PRIORITY_DESCRIPTION,
        INSERT_DT,
        UPDATE_DT)
    SELECT
        nextval('bl_dm.dim_ORDER_PRIORITIES_SEQ') AS ORDER_PRIORITY_SURR_ID,
        COALESCE(cte.ORDER_PRIORITY_SRC_ID::text, (SELECT ORDER_PRIORITY_SRC_ID FROM BL_DM.DIM_ORDER_PRIORITIES dop WHERE dop.ORDER_PRIORITY_SURR_ID = -1)) AS ORDER_PRIORITY_SRC_ID,
        COALESCE(cte.SOURCE_SYSTEM, (SELECT SOURCE_SYSTEM FROM BL_DM.DIM_ORDER_PRIORITIES dop WHERE dop.ORDER_PRIORITY_SURR_ID = -1)) AS SOURCE_SYSTEM,
        COALESCE(cte.SOURCE_TABLE, (SELECT SOURCE_TABLE FROM BL_DM.DIM_ORDER_PRIORITIES dop WHERE dop.ORDER_PRIORITY_SURR_ID = -1)) AS SOURCE_TABLE,
        COALESCE(UPPER(cte.ORDER_PRIORITY_NAME), (SELECT ORDER_PRIORITY_NAME FROM BL_DM.DIM_ORDER_PRIORITIES dop WHERE dop.ORDER_PRIORITY_SURR_ID = -1)) AS ORDER_PRIORITY_NAME,
        COALESCE(cte.ORDER_PRIORITY_DESCRIPTION, (SELECT ORDER_PRIORITY_DESCRIPTION FROM BL_DM.DIM_ORDER_PRIORITIES dop WHERE dop.ORDER_PRIORITY_SURR_ID = -1)) AS ORDER_PRIORITY_DESCRIPTION,  
        COALESCE(CURRENT_DATE, (SELECT INSERT_DT FROM BL_DM.DIM_ORDER_PRIORITIES dop WHERE dop.ORDER_PRIORITY_SURR_ID = -1)) AS INSERT_DT,
        COALESCE(CURRENT_DATE, (SELECT UPDATE_DT FROM BL_DM.DIM_ORDER_PRIORITIES dop WHERE dop.ORDER_PRIORITY_SURR_ID = -1)) AS UPDATE_DT
    FROM cte
    WHERE NOT EXISTS (
        SELECT 1
        FROM BL_DM.DIM_ORDER_PRIORITIES dop
        WHERE UPPER(cte.ORDER_PRIORITY_NAME) = UPPER(dop.ORDER_PRIORITY_NAME)
        AND UPPER(dop.ORDER_PRIORITY_SRC_ID) = UPPER(cte.ORDER_PRIORITY_SRC_ID::text)
        AND UPPER(dop.SOURCE_SYSTEM) = UPPER(cte.SOURCE_SYSTEM)
        AND UPPER(dop.SOURCE_TABLE) = UPPER(cte.SOURCE_TABLE)
    );

    GET DIAGNOSTICS num_rows_affected = ROW_COUNT;
    log_message := 'Procedure LD_DIM_ORDER_PRIORITIES completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, num_rows_affected, log_message) VALUES ('LD_DIM_ORDER_PRIORITIES', num_rows_affected, log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in LD_DIM_ORDER_PRIORITIES: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('LD_DIM_ORDER_PRIORITIES', log_message);
        RAISE;
END;
$$;








CREATE OR REPLACE PROCEDURE BL_DM.LD_DIM_DATES()
LANGUAGE plpgsql
AS $$
DECLARE
    num_rows_affected integer;
    log_message TEXT;
BEGIN
    INSERT INTO BL_DM.DIM_dates (date_id, day_name, day_number_in_week, calendar_week_number, calendar_month_number,
                          calendar_month_name, days_in_cal_month, end_day_of_cal_month,
                          calendar_quarter_number, calendar_year, days_in_cal_year, end_of_cal_year)
    SELECT
        date_id,
        TO_CHAR(date_id, 'Day'),
        EXTRACT(DOW FROM date_id) + 1,
        EXTRACT(WEEK FROM date_id),
        EXTRACT(MONTH FROM date_id),
        TO_CHAR(date_id, 'Month'),
        EXTRACT(DAY FROM (date_trunc('month', date_id) + INTERVAL '1 month' - INTERVAL '1 day')),
        EXTRACT(DAY FROM (date_trunc('month', date_id) + INTERVAL '1 month' - INTERVAL '1 day')),
        EXTRACT(QUARTER FROM date_id),
        EXTRACT(YEAR FROM date_id),
        EXTRACT(DOY FROM (date_trunc('year', date_id) + INTERVAL '1 year' - INTERVAL '1 day')),
        (EXTRACT(DAY FROM date_trunc('year', date_id) + INTERVAL '1 year' - INTERVAL '1 day'))
    FROM
        generate_series('2022-01-01'::DATE, '2023-12-31'::DATE, '1 day') AS date_id
    WHERE NOT EXISTS (
        SELECT 1
        FROM BL_DM.DIM_dates
        WHERE dim_dates.date_id = date_id);

    GET DIAGNOSTICS num_rows_affected = ROW_COUNT;
    log_message := 'Procedure LD_DIM_DATES completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, num_rows_affected, log_message) VALUES ('LD_DIM_DATES', num_rows_affected, log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in LD_DIM_DATES: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('LD_DIM_DATES', log_message);
        RAISE;
END;
$$;






CREATE OR REPLACE PROCEDURE BL_DM.LD_DIM_STORES_SCD()
LANGUAGE plpgsql
AS $$
DECLARE
    temp_row record;
    num_rows_affected integer := 0;
    num integer;
    log_message TEXT;
BEGIN
    CREATE TEMP TABLE temp_table AS
    WITH cte AS (
        SELECT 
            css.STORE_ID AS STORE_SRC_ID,
            'BL_3NF' AS SOURCE_SYSTEM,
            'CE_STORES_SCD <- CE_STORE_SIZES <- CE_ADDRESSES <- CE_CITIES <- CE_COUNTRIES <- CE_REGIONS' AS SOURCE_TABLE,
            csz.STORE_SIZE_ID,
            csz.store_size_name,
            csz.store_size_description,
            ca.address_id AS STORE_ADDRESS_ID,
            ca.address_name AS STORE_ADDRESS_NAME,
            cct.city_id AS STORE_CITY_ID,
            cct.city_name AS STORE_CITY_NAME,
            cc.country_id AS STORE_COUNTRY_ID,
            cc.country_name AS STORE_COUNTRY_NAME,
            cr.region_id AS STORE_REGION_ID,
            cr.region_name AS STORE_REGION_NAME,
            css.start_dt AS START_DT,
            css.is_active AS IS_ACTIVE
        FROM BL_3NF.CE_STORES_SCD css
        LEFT JOIN BL_3NF.CE_STORE_SIZES csz ON css.store_size_id = csz.store_size_id
        LEFT JOIN BL_3NF.CE_ADDRESSES ca ON ca.address_id = CSS.store_address_id 
        LEFT JOIN BL_3NF.CE_CITIES cct ON ca.city_id = cct.city_id 
        LEFT JOIN BL_3NF.CE_COUNTRIES cc ON cct.country_id = cc.country_id 
        LEFT JOIN BL_3NF.CE_REGIONS cr ON cc.region_id = cr.region_id 
        WHERE css.STORE_ID <> -1
       ) SELECT * FROM cte;
    FOR temp_row IN SELECT * FROM temp_table
    LOOP
    IF EXISTS (
        SELECT 1
        FROM BL_DM.DIM_STORES_SCD dss
        WHERE dss.store_address_id = temp_row.store_address_id
        AND dss.store_size_id <> temp_row.store_size_id AND  temp_row.IS_ACTIVE = 'Y'
        AND UPPER(dss.STORE_SRC_ID) = UPPER(temp_row.STORE_SRC_ID::text)
        AND UPPER(dss.SOURCE_SYSTEM) = UPPER(temp_row.SOURCE_SYSTEM)
        AND UPPER(dss.SOURCE_TABLE) = UPPER(temp_row.SOURCE_TABLE)
    ) THEN
        UPDATE DIM_STORES_SCD dss
        SET IS_ACTIVE = 'N', END_DT = current_date
        WHERE dss.store_address_id = temp_row.store_address_id
        AND dss.store_size_id <> temp_row.store_size_id AND temp_row.IS_ACTIVE = 'Y'
        AND UPPER(dss.STORE_SRC_ID) = UPPER(temp_row.STORE_SRC_ID::text)
        AND UPPER(dss.SOURCE_SYSTEM) = UPPER(temp_row.SOURCE_SYSTEM)
        AND UPPER(dss.SOURCE_TABLE) = UPPER(temp_row.SOURCE_TABLE);
       RAISE NOTICE 'success1';
	    INSERT INTO BL_DM.DIM_STORES_SCD
		    (STORE_SURR_ID,
		    STORE_SRC_ID,
		    SOURCE_SYSTEM,
		    SOURCE_TABLE,
		    STORE_SIZE_ID,
		    STORE_SIZE_NAME,
		    STORE_SIZE_DESCRIPTION,
		    STORE_ADDRESS_ID,
		    STORE_ADDRESS_NAME,
		    STORE_CITY_ID,
		    STORE_CITY_NAME,
		    STORE_COUNTRY_ID,
		    STORE_COUNTRY_NAME,
		    STORE_REGION_ID,
		    STORE_REGION_NAME,
		    START_DT,
		    END_DT,
		    IS_ACTIVE,
		    INSERT_DT)
	    SELECT
	        (SELECT max(STORE_SURR_ID) 
	        FROM BL_DM.DIM_STORES_SCD dss
	        LEFT JOIN temp_table tt ON dss.store_address_id = tt.store_address_id
	        WHERE dss.store_size_id <> tt.store_size_id AND  tt.IS_ACTIVE = 'N') AS STORE_SURR_ID,
	        COALESCE(temp_row.STORE_SRC_ID::text, (SELECT STORE_SRC_ID FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS STORE_SRC_ID,
	        COALESCE(temp_row.SOURCE_SYSTEM, (SELECT SOURCE_SYSTEM FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS SOURCE_SYSTEM,
	        COALESCE(temp_row.SOURCE_TABLE, (SELECT SOURCE_TABLE FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS SOURCE_TABLE,
	        COALESCE(temp_row.STORE_SIZE_ID, (SELECT STORE_SIZE_ID FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS STORE_SIZE_ID,
	        COALESCE(UPPER(temp_row.STORE_SIZE_NAME), (SELECT STORE_SIZE_NAME FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS STORE_SIZE_NAME,
	        COALESCE(temp_row.STORE_SIZE_DESCRIPTION, (SELECT STORE_SIZE_DESCRIPTION FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS STORE_SIZE_DESCRIPTION,
	        COALESCE(temp_row.STORE_ADDRESS_ID, (SELECT STORE_ADDRESS_ID FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS STORE_ADDRESS_ID,  
	        COALESCE(UPPER(temp_row.STORE_ADDRESS_NAME), (SELECT STORE_ADDRESS_NAME FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS STORE_ADDRESS_NAME,
	        COALESCE(temp_row.STORE_CITY_ID, (SELECT STORE_CITY_ID FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS STORE_CITY_ID,  
	        COALESCE(UPPER(temp_row.STORE_CITY_NAME), (SELECT STORE_CITY_NAME FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS STORE_CITY_NAME,
	        COALESCE(temp_row.STORE_COUNTRY_ID, (SELECT STORE_COUNTRY_ID FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS STORE_COUNTRY_ID,  
	        COALESCE(UPPER(temp_row.STORE_COUNTRY_NAME), (SELECT STORE_COUNTRY_NAME FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS STORE_COUNTRY_NAME,
	        COALESCE(temp_row.STORE_REGION_ID, (SELECT STORE_REGION_ID FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS STORE_REGION_ID,  
	        COALESCE(UPPER(temp_row.STORE_REGION_NAME), (SELECT STORE_REGION_NAME FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS STORE_REGION_NAME,
	        COALESCE(CURRENT_DATE, (SELECT START_DT FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS START_DT,
	        COALESCE('9999-12-31', (SELECT END_DT FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS END_DT,
	        COALESCE('Y', (SELECT IS_ACTIVE FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS IS_ACTIVE,
	        COALESCE(CURRENT_DATE, (SELECT INSERT_DT FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS INSERT_DT
	       WHERE NOT EXISTS 
	        ( SELECT 1
		     FROM BL_DM.DIM_STORES_SCD dss
		     WHERE dss.STORE_ADDRESS_ID = temp_row.STORE_ADDRESS_ID
		     AND UPPER(dss.STORE_SRC_ID) = UPPER(temp_row.STORE_SRC_ID::text)
		     AND UPPER(dss.SOURCE_SYSTEM) = UPPER(temp_row.SOURCE_SYSTEM)
		     AND UPPER(dss.SOURCE_TABLE) = UPPER(temp_row.SOURCE_TABLE));
	       RAISE NOTICE 'success2';
        GET DIAGNOSTICS num = ROW_COUNT;
		num_rows_affected := num_rows_affected + num;
    ELSE
	    INSERT INTO BL_DM.DIM_STORES_SCD
	    (STORE_SURR_ID,
	    STORE_SRC_ID,
	    SOURCE_SYSTEM,
	    SOURCE_TABLE,
	    STORE_SIZE_ID,
	    STORE_SIZE_NAME,
	    STORE_SIZE_DESCRIPTION,
	    STORE_ADDRESS_ID,
	    STORE_ADDRESS_NAME,
	    STORE_CITY_ID,
	    STORE_CITY_NAME,
	    STORE_COUNTRY_ID,
	    STORE_COUNTRY_NAME,
	    STORE_REGION_ID,
	    STORE_REGION_NAME,
	    START_DT,
	    END_DT,
	    IS_ACTIVE,
	    INSERT_DT)
	    SELECT
        nextval('bl_dm.dim_STORES_SCD_SEQ') AS STORE_SURR_ID,
        COALESCE(temp_row.STORE_SRC_ID::text, (SELECT STORE_SRC_ID FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS STORE_SRC_ID,
        COALESCE(temp_row.SOURCE_SYSTEM, (SELECT SOURCE_SYSTEM FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS SOURCE_SYSTEM,
        COALESCE(temp_row.SOURCE_TABLE, (SELECT SOURCE_TABLE FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS SOURCE_TABLE,
        COALESCE(temp_row.STORE_SIZE_ID, (SELECT STORE_SIZE_ID FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS STORE_SIZE_ID,
        COALESCE(UPPER(temp_row.STORE_SIZE_NAME), (SELECT STORE_SIZE_NAME FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS STORE_SIZE_NAME,
        COALESCE(temp_row.STORE_SIZE_DESCRIPTION, (SELECT STORE_SIZE_DESCRIPTION FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS STORE_SIZE_DESCRIPTION,
        COALESCE(temp_row.STORE_ADDRESS_ID, (SELECT STORE_ADDRESS_ID FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS STORE_ADDRESS_ID,  
        COALESCE(UPPER(temp_row.STORE_ADDRESS_NAME), (SELECT STORE_ADDRESS_NAME FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS STORE_ADDRESS_NAME,
        COALESCE(temp_row.STORE_CITY_ID, (SELECT STORE_CITY_ID FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS STORE_CITY_ID,  
        COALESCE(UPPER(temp_row.STORE_CITY_NAME), (SELECT STORE_CITY_NAME FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS STORE_CITY_NAME,
        COALESCE(temp_row.STORE_COUNTRY_ID, (SELECT STORE_COUNTRY_ID FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS STORE_COUNTRY_ID,  
        COALESCE(UPPER(temp_row.STORE_COUNTRY_NAME), (SELECT STORE_COUNTRY_NAME FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS STORE_COUNTRY_NAME,
        COALESCE(temp_row.STORE_REGION_ID, (SELECT STORE_REGION_ID FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS STORE_REGION_ID,  
        COALESCE(UPPER(temp_row.STORE_REGION_NAME), (SELECT STORE_REGION_NAME FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS STORE_REGION_NAME,
        COALESCE(temp_row.START_DT, (SELECT START_DT FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS START_DT,
        COALESCE('9999-12-31', (SELECT END_DT FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS END_DT,
        COALESCE('Y', (SELECT IS_ACTIVE FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS IS_ACTIVE,
        COALESCE(CURRENT_DATE, (SELECT INSERT_DT FROM BL_DM.DIM_STORES_SCD dss WHERE dss.STORE_SURR_ID = -1)) AS INSERT_DT
       WHERE NOT EXISTS 
      ( SELECT 1
     FROM BL_DM.DIM_STORES_SCD dss
     WHERE dss.STORE_ADDRESS_ID = temp_row.STORE_ADDRESS_ID
     AND UPPER(dss.STORE_SRC_ID) = UPPER(temp_row.STORE_SRC_ID::text)
     AND UPPER(dss.SOURCE_SYSTEM) = UPPER(temp_row.SOURCE_SYSTEM)
     AND UPPER(dss.SOURCE_TABLE) = UPPER(temp_row.SOURCE_TABLE));
    RAISE NOTICE 'success3';
        GET DIAGNOSTICS num = ROW_COUNT;
		num_rows_affected := num_rows_affected + num;
        END IF;

    END LOOP;
    
   log_message := 'Procedure LD_DIM_STORES_SCD completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, num_rows_affected, log_message) VALUES ('LD_DIM_STORES_SCD', num_rows_affected, log_message);

    DROP TABLE temp_table;
   EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in LD_DIM_STORES_SCD: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('LD_DIM_STORES_SCD', log_message);
        RAISE;
END;
$$;

	   

CREATE OR REPLACE PROCEDURE LD_FCT_SALES(p_year INT DEFAULT NULL, p_month INT DEFAULT NULL)
LANGUAGE plpgsql
AS $$
DECLARE
    partition_exists boolean;
    num_rows_affected integer;
    log_message TEXT;
    table_name TEXT;
   	target_table_name TEXT;
    start_date date;
    end_date date;
BEGIN
	IF p_year IS NULL OR p_month IS NULL THEN
		table_name := 'ce_sales';
		target_table_name := 'fct_sales';
	ELSE 
    	table_name := 'ce_sales_' || p_year || '_' || LPAD(p_month::TEXT, 2, '0');
    	target_table_name := 'fct_sales_' || p_year || '_' || LPAD(p_month::TEXT, 2, '0');
    	start_date := DATE(p_year || '-' || p_month || '-01');
        end_date := start_date + INTERVAL '1 month';
	END IF;
    SELECT EXISTS(
             SELECT * FROM information_schema.tables t
            WHERE lower(t.table_name) = lower(target_table_name)
    ) INTO partition_exists;
EXECUTE '
    CREATE TABLE IF NOT EXISTS BL_DM.'||quote_ident(target_table_name)||'
	  (LIKE fct_sales INCLUDING DEFAULTS INCLUDING CONSTRAINTS)';

EXECUTE format('
		INSERT INTO BL_DM.%s
			(SALE_SURR_ID,
			SALES_CHANNEL_ID,
			STORE_ID,
			ITEM_ID,
			ORDERING_WAY_ID,
			ORDER_PRIORITY_ID,
			ORDER_DATE_ID,
			PAYMENT_METHOD_ID,
			CUSTOMER_TYPE_ID,
			PROMOTION_TYPE_ID,
			SALE_PERCENTAGE,
			UNITS_SOLD,
			UNIT_PRICE,
			UNIT_COST,
			TOTAL_PRICE,
			TOTAL_REVENUE,
			INSERT_DT)
		SELECT SALE_ID AS SALE_SURR_ID,
			SALES_CHANNEL_ID,
			STORE_ID,
			ITEM_ID,
			ORDERING_WAY_ID,
			ORDER_PRIORITY_ID,
			DD.DATE_ID AS ORDER_DATE_ID,
			PAYMENT_METHOD_ID,
			CUSTOMER_TYPE_ID,
			PROMOTION_TYPE_ID,
			DPT.SALE_PERCENTAGE,
			UNITS_SOLD,
			UNIT_PRICE,
			UNIT_COST,
			UNITS_SOLD*UNIT_PRICE*(1-DPT.SALE_PERCENTAGE/100) AS TOTAL_PRICE,
			UNITS_SOLD*(UNIT_PRICE-UNIT_COST)*(1-DPT.SALE_PERCENTAGE/100) AS TOTAL_REVENUE,
			current_date AS INSERT_DT
		FROM BL_3NF.CE_SALES CS
		JOIN BL_DM.DIM_DATES DD ON CS.ORDER_DATE = DD.DATE_ID
		JOIN BL_DM.DIM_PROMOTION_TYPES DPT ON UPPER(CS.PROMOTION_TYPE_ID::TEXT) = DPT.PROMOTION_TYPE_SRC_ID
		WHERE NOT EXISTS (
		SELECT 1
		FROM FCT_SALES FS
		WHERE CS.SALE_ID = FS.SALE_SURR_ID
		AND CS.ORDER_DATE = FS.ORDER_DATE_ID)', quote_ident(target_table_name));
	
	IF NOT partition_exists THEN
	EXECUTE format('ALTER TABLE BL_DM.FCT_SALES ATTACH PARTITION BL_DM.%s
    FOR VALUES FROM ('''||start_date||''') TO ('''||end_date||''')', quote_ident(target_table_name));
   	END IF;
	GET DIAGNOSTICS num_rows_affected = ROW_COUNT;
    log_message := 'Procedure LD_FCT_SALES completed successfully.';
    INSERT INTO sa_sales.log_table (procedure_name, num_rows_affected, log_message) VALUES ('LD_FCT_SALES', num_rows_affected, log_message);

EXCEPTION
    WHEN OTHERS THEN
        log_message := 'Error in LD_FCT_SALES: ' || SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message) VALUES ('LD_FCT_SALES', log_message);
        RAISE;
END;
$$;





CREATE OR REPLACE PROCEDURE LOAD_ALL_BL_DM_TABLES()
LANGUAGE plpgsql
AS $$
BEGIN
	CALL BL_DM.LD_DIM_CUSTOMER_TYPES();
	CALL BL_DM.LD_DIM_PAYMENT_METHODS();
	CALL BL_DM.LD_DIM_ORDERING_WAYS();
	CALL BL_DM.LD_DIM_SALES_CHANNELS();
	CALL BL_DM.LD_DIM_PROMOTION_TYPES();
	CALL BL_DM.LD_DIM_ITEMS();
	CALL BL_DM.LD_DIM_ORDER_PRIORITIES();
	CALL BL_DM.LD_DIM_DATES();
	CALL BL_DM.LD_DIM_ITEMS();

	CALL BL_DM.LD_DIM_STORES_SCD();
    INSERT INTO sa_sales.log_table (procedure_name, log_message)
    VALUES ('LOAD_ALL_BL_DM_TABLES', 'All procedures completed successfully.');
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error in LOAD_ALL_BL_DM_TABLES: %', SQLERRM;
        INSERT INTO sa_sales.log_table (procedure_name, log_message)
        VALUES ('LOAD_ALL_BL_DM_TABLES', 'Error: ' || SQLERRM);
        RAISE;
END;
$$;




CALL LOAD_ALL_BL_DM_TABLES();
    
CALL BL_DM.LD_FCT_SALES(2022,1);


COMMIT;



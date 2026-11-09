CREATE OR REPLACE FUNCTION bd_shop_dm.refresh_dm_sales(
    p_critical_sales double precision DEFAULT 250000
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_rev_date date;
    v_shop_id bigint;
    v_sales_volume double precision;
BEGIN
    IF p_critical_sales IS NULL OR p_critical_sales < 0 THEN
        RAISE EXCEPTION 'Critical sales limit must be a non-negative number';
    END IF;

    SELECT
        p.revizion_date,
        p.shop_id,
        SUM(p.count * p.price_sale_out)
    INTO v_rev_date, v_shop_id, v_sales_volume
    FROM bd_shops.product AS p
    GROUP BY p.revizion_date, p.shop_id
    HAVING SUM(p.count * p.price_sale_out) > p_critical_sales
    ORDER BY SUM(p.count * p.price_sale_out) DESC
    LIMIT 1;

    IF FOUND THEN
        RAISE EXCEPTION
            'Critical sales limit exceeded: date=%, shop_id=%, sales_volume=%, limit=%',
            v_rev_date, v_shop_id, v_sales_volume, p_critical_sales;
    END IF;

    TRUNCATE TABLE bd_shop_dm.dm_sales;

    INSERT INTO bd_shop_dm.dm_sales (rev_date, shop_id, sales_volume)
    SELECT
        p.revizion_date,
        p.shop_id,
        SUM(p.count * p.price_sale_out)
    FROM bd_shops.product AS p
    GROUP BY p.revizion_date, p.shop_id;
END;
$$;
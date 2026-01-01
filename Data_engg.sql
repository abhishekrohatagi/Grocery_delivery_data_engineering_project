
/* ============================================================================
   - Creates database & schemas
   - Loads bronze CSVs (BULK INSERT)
   - Builds silver table by joining bronze tables (only stores with city mapping)
   - Computes est_qty_sold using LEAD() & 3-period historical average for restocks
   - Computes both wt_osa (global / total dark stores) and wt_osa_ls (per-sku listed %)
   - Produces gold.blinkit_city_insights (final derived table)
   ============================================================================ */

use master;
go

-- drop & recreate db
if exists (select 1 from sys.databases where name = 'blinkit_analytics')
begin
    alter database blinkit_analytics set single_user with rollback immediate;
    drop database blinkit_analytics;
end;
go

create database blinkit_analytics;
go

use blinkit_analytics;
go

-- create schemas if not exists
if schema_id('bronze') is null exec('create schema bronze');
if schema_id('silver') is null exec('create schema silver');
if schema_id('gold') is null   exec('create schema gold');
go

-- =========================
-- bronze tables
-- =========================
if object_id('bronze.all_blinkit_category_scraping_stream','u') is not null drop table bronze.all_blinkit_category_scraping_stream;
go
create table bronze.all_blinkit_category_scraping_stream (
    created_at datetime,
    l1_category_id int,
    l2_category_id int,
    store_id int,
    sku_id varchar(100),
    sku_name nvarchar(255),
    selling_price float,
    mrp float,
    inventory int,
    image_url nvarchar(500),
    brand_id int,
    brand nvarchar(255),
    unit nvarchar(50)
);
go

if object_id('bronze.blinkit_categories','u') is not null drop table bronze.blinkit_categories;
go
create table bronze.blinkit_categories (
    l1_category nvarchar(255),
    l1_category_id int,
    l2_category nvarchar(255),
    l2_category_id int
);
go

if object_id('bronze.blinkit_city_map','u') is not null drop table bronze.blinkit_city_map;
go
create table bronze.blinkit_city_map (
    store_id int,
    city_name nvarchar(255)
);
go

-- truncate if necessary (safe to run if empty)
truncate table bronze.all_blinkit_category_scraping_stream;
truncate table bronze.blinkit_categories;
truncate table bronze.blinkit_city_map;
go

-- adjust file paths before running
bulk insert bronze.all_blinkit_category_scraping_stream
from 'c:\users\abhishek rohatagi\downloads\blinkit\all_blinkit_category_scraping_stream.csv'
with (
    firstrow = 2,
    fieldterminator = ',',
    rowterminator = '0x0a',
    format = 'csv',
    tablock,
    codepage = '65001'
);
go

bulk insert bronze.blinkit_categories
from 'c:\users\abhishek rohatagi\downloads\blinkit\blinkit_categories.csv'
with (
    firstrow = 2,
    fieldterminator = ',',
    rowterminator = '0x0a',
    format = 'csv',
    tablock,
    codepage = '65001'
);
go

bulk insert bronze.blinkit_city_map
from 'c:\users\abhishek rohatagi\downloads\blinkit\blinkit_city_map.csv'
with (
    firstrow = 2,
    fieldterminator = ',',
    rowterminator = '0x0a',
    format = 'csv',
    tablock,
    codepage = '65001'
);
go

-- =========================
-- silver consolidated table
-- =========================
if object_id('silver.all_blinkit_category_data','u') is not null drop table silver.all_blinkit_category_data;
go

create table silver.all_blinkit_category_data (
    date datetime,
    brand_id int,
    brand nvarchar(255),
    image_url nvarchar(500),
    city_name nvarchar(255),
    sku_id varchar(100),
    sku_name nvarchar(255),
    category_id int,
    category_name nvarchar(255),
    sub_category_id int,
    sub_category_name nvarchar(255),
    mrp float,
    selling_price float,
    store_id int,
    inventory int
);
go

insert into silver.all_blinkit_category_data (
    date,
    brand_id,
    brand,
    image_url,
    city_name,
    sku_id,
    sku_name,
    category_id,
    category_name,
    sub_category_id,
    sub_category_name,
    mrp,
    selling_price,
    store_id,
    inventory
)
select
    b.created_at as date,
    b.brand_id,
    b.brand,
    b.image_url,
    m.city_name,
    b.sku_id,
    b.sku_name,
    cat.l1_category_id as category_id,
    cat.l1_category as category_name,
    cat.l2_category_id as sub_category_id,
    cat.l2_category as sub_category_name,
    b.mrp,
    b.selling_price,
    b.store_id,
    b.inventory
from bronze.all_blinkit_category_scraping_stream b
inner join bronze.blinkit_city_map m
    on b.store_id = m.store_id            -- exclude stores without city mapping (as required)
inner join bronze.blinkit_categories cat
    on b.l1_category_id = cat.l1_category_id
   and b.l2_category_id = cat.l2_category_id;
go

-- =========================
-- gold: derived metrics + final table
-- =========================

if object_id('gold.blinkit_city_insights','u') is not null drop table gold.blinkit_city_insights;
go

with
-- inventory timeline with next snapshot (lead)
cte_inventory_with_lead as (
    select
        date,
        brand_id,
        brand,
        image_url,
        city_name,
        sku_id,
        sku_name,
        category_id,
        category_name,
        sub_category_id,
        sub_category_name,
        mrp,
        selling_price,
        store_id,
        inventory,
        lead(inventory) over (partition by store_id, sku_id order by date) as next_inventory
    from silver.all_blinkit_category_data
),

-- sold quantity when inventory decreases (actual sold between two snapshots)
cte_sold_quantity as (
    select
        *,
        case 
            when next_inventory is not null and next_inventory < inventory then (inventory - next_inventory)
            else null
        end as sold_qty
    from cte_inventory_with_lead
),

-- 3-period historical average of sold_qty (previous 3 snapshots). avg ignores nulls.
cte_avg_sales as (
    select
        *,
        avg(sold_qty) over (
            partition by store_id, sku_id
            order by date
            rows between 3 preceding and 1 preceding
        ) as avg_prev_3_sales
    from cte_sold_quantity
),

-- final per-row estimated qty sold: actual if decrease, avg_prev_3_sales if restock, 0 if no change
cte_estimated_sales as (
    select
        date,
        brand_id,
        brand,
        image_url,
        city_name,
        sku_id,
        sku_name,
        category_id,
        category_name,
        sub_category_id,
        sub_category_name,
        mrp,
        selling_price,
        store_id,
        inventory,
        next_inventory,
        case
            when next_inventory is not null and next_inventory < inventory then (inventory - next_inventory)    -- actual sales
            when next_inventory is not null and next_inventory > inventory then coalesce(avg_prev_3_sales, 0)   -- restock: estimate using historical avg
            when next_inventory is not null and next_inventory = inventory then 0                               -- no change
            else 0  -- if no next snapshot (last record), assume 0 sold for that interval
        end as est_qty_sold
    from cte_avg_sales
),

-- sku-level daily summary (aggregated by date, city & sku)
cte_sku_level_summary as (
    select
        cast(date as date) as date,
        city_name,
        sku_id,
        sku_name,
        brand_id,
        brand,
        image_url,
        category_id,
        category_name,
        sub_category_id,
        sub_category_name,
        round(avg(case when mrp is not null and mrp <> 0 then ((mrp - selling_price) / mrp) end), 4) as discount,
        sum(inventory) as inventory,
        sum(est_qty_sold) as est_qty_sold,
        sum(est_qty_sold * coalesce(mrp, 0)) as est_sales_mrp,
        sum(est_qty_sold * coalesce(selling_price, 0)) as est_sales_sp
    from cte_estimated_sales
    group by
        cast(date as date),
        city_name,
        sku_id,
        sku_name,
        brand_id,
        brand,
        image_url,
        category_id,
        category_name,
        sub_category_id,
        sub_category_name
),

-- per-sku availability: stores_in_stock (distinct stores where inventory > 0)
sku_availability as (
    select 
        sku_id,
        count(distinct case when inventory > 0 then store_id end) as stores_in_stock,
        count(distinct store_id) as total_listed_stores_per_sku
    from silver.all_blinkit_category_data
    group by sku_id
),

-- total listed dark stores per sku from bronze (all unique stores carrying this sku)
cte_dark_stores_per_sku as (
    select
        sku_id,
        count(distinct store_id) as total_listed_dark_stores
    from bronze.all_blinkit_category_scraping_stream
    group by sku_id
),

-- weighted osa per-sku using listed stores (per-sku denominator)
cte_weighted_osa_ls as (
    select
        sku_id,
        case when total_listed_stores_per_sku = 0 then 0
             else (cast(stores_in_stock as float) / cast(total_listed_stores_per_sku as float)) * 100
        end as wt_osa_ls
    from sku_availability
),

-- total number of unique dark stores (from bronze for global total)
total_stores as (
    select count(distinct store_id) as total_dark_stores
    from bronze.all_blinkit_category_scraping_stream
),

-- weighted osa using total dark stores (global denominator)
cte_wt_osa_global as (
    select 
        s.sku_id,
        s.stores_in_stock,
        t.total_dark_stores,
        case when t.total_dark_stores = 0 then 0
             else (cast(s.stores_in_stock as float) / cast(t.total_dark_stores as float)) * 100
        end as wt_osa
    from sku_availability s
    cross join total_stores t
),

-- per-sku mode of mrp (most frequently observed mrp per sku)
cte_mrp_mode as (
    select sku_id, mrp as mrp_mode
    from (
        select
            sku_id,
            mrp,
            count(*) as cnt,
            row_number() over (partition by sku_id order by count(*) desc, mrp) as rn
        from silver.all_blinkit_category_data
        group by sku_id, mrp
    ) t
    where rn = 1
),

-- per-sku mode of selling_price (most frequently observed selling_price per sku)
cte_sp_mode as (
    select sku_id, selling_price as selling_price_mode
    from (
        select
            sku_id,
            selling_price,
            count(*) as cnt,
            row_number() over (partition by sku_id order by count(*) desc, selling_price) as rn
        from silver.all_blinkit_category_data
        group by sku_id, selling_price
    ) t
    where rn = 1
)

--  create gold.blinkit_city_insights using results above
select
    s.date,
    s.brand_id,
    s.brand,
    s.image_url,
    s.city_name,
    s.sku_id,
    s.sku_name,
    s.category_id,
    s.category_name,
    s.sub_category_id,
    s.sub_category_name,
    s.est_qty_sold,
    s.est_sales_sp,
    s.est_sales_mrp,
    s.inventory,
    sa.total_listed_stores_per_sku as listed_ds_count,
    ds.total_listed_dark_stores as ds_count, 
    osa_ls.wt_osa_ls,
    osa_global.wt_osa,
    mrp.mrp_mode,
    sp.selling_price_mode,
    s.discount
into gold.blinkit_city_insights
from cte_sku_level_summary s
left join sku_availability sa
    on sa.sku_id = s.sku_id
left join cte_weighted_osa_ls osa_ls
    on osa_ls.sku_id = s.sku_id
left join cte_wt_osa_global osa_global
    on osa_global.sku_id = s.sku_id
left join cte_dark_stores_per_sku ds
    on ds.sku_id = s.sku_id
left join cte_mrp_mode mrp
    on mrp.sku_id = s.sku_id
left join cte_sp_mode sp
    on sp.sku_id = s.sku_id;
go

-- checks
select * from gold.blinkit_city_insights order by date desc;
go



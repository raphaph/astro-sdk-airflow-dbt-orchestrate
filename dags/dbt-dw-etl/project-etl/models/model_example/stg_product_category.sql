{{ config(
    tags=["stage","etl"]
) }}

select 
    cast(product_category_name as varchar(200)) as product_category_name,
    cast(product_category_name_en as varchar(200)) as product_category_name_en

from {{source('dl_astro','tb_astro_category')}}
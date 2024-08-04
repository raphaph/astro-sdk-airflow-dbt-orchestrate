{{ config(
    materialized='incremental',
    unique_key="product_category_name",
    sort_key="product_category_name",
    tags=["stage","etl"]
    )}}

-- exemplo apenas, relacionamento ou tratamento são feitos nessa camada em casos reais
select * 
from {{ ref('stg_product_category') }}
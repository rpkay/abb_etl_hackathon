-- auto-generated definition
create table fact_sales
(
    sale_key    bigserial
        primary key,
    product_key bigint
        constraint fk_product
            references dim_product,
    region_key  bigint
        constraint fk_region
            references dim_region,
    date_key    bigint
        constraint fk_date
            references dim_date,
    quantity    integer,
    price       numeric(10, 2),
    total       numeric(12, 2)
);

alter table fact_sales
    owner to admin;


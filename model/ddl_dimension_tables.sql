-- Dimension Table: dim_date

create table dim_date
(
    date_key   bigserial
        primary key,
    full_date  date,
    year       integer,
    month      integer,
    month_name varchar(20),
    quarter    varchar(10)
);

-- Dimension Table: dim_product

create table dim_product
(
    product_key    bigserial
        primary key,
    product_name   varchar(100),
    category       varchar(50),
    manufacturer   varchar(50),
    warranty_years integer,
    start_date     date,
    end_date       date,
    is_current     boolean
);

-- Dimension Table: dim_region

create table dim_region
(
    region_key       bigserial
        primary key,
    region_name      varchar(50),
    manager          varchar(50),
    established_year integer
);






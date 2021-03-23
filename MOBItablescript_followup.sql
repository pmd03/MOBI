
--select * from integration.staged_fx_rate;
	
-- Table: integration.staged_fx_rate

-- DROP TABLE IF EXISTS integration.staged_fx_rate;

CREATE TABLE integration.staged_fx_rate
(
    fx_date timestamp without time zone,
    from_ccy character varying(3) COLLATE pg_catalog."default" NOT NULL,
    to_ccy character varying(3) COLLATE pg_catalog."default" NOT NULL,
    fx_rate numeric(10,3) NOT NULL
)

TABLESPACE pg_default;

ALTER TABLE integration.staged_fx_rate
    OWNER to postgres;
	
	
select from_ccy,max(fx_rate) 
from integration.staged_fx_rate
group by from_ccy order by max(fx_rate) desc;


-- Table: integration.staged_closing_price

-- DROP TABLE IF EXISTS integration.staged_closing_price;

CREATE TABLE integration.staged_closing_price
(
    ccy text COLLATE pg_catalog."default",
    day_high_price numeric(10,4),
    day_low_price numeric(10,4),
    eod_price numeric(10,4),
	day_high_price_converted numeric(10,4),
    day_low_price_converted numeric(10,4),
    eod_price_converted numeric(10,4),
    fininstrmgnlattrbts_id text COLLATE pg_catalog."default",
    trd_dt date
)

TABLESPACE pg_default;

ALTER TABLE integration.staged_closing_price
    OWNER to postgres;
	

-- Table: integration.staged_instrument

-- DROP TABLE IF EXISTS integration.staged_instrument;

CREATE TABLE integration.staged_instrument
(
    instrm_id text COLLATE pg_catalog."default" NOT NULL,
    industry_nm text COLLATE pg_catalog."default" NOT NULL,
    type text COLLATE pg_catalog."default" NOT NULL,
    instrm_type text COLLATE pg_catalog."default" NOT NULL,
    instrm_fullnm text COLLATE pg_catalog."default" NOT NULL,
    instrm_shrtnm text COLLATE pg_catalog."default" NOT NULL,
    instrm_clssfctntp text COLLATE pg_catalog."default" NOT NULL,
    instrm_ntnlccy text COLLATE pg_catalog."default" NOT NULL,
    instrm_cmmdtyderivind text COLLATE pg_catalog."default" NOT NULL,
    issr text COLLATE pg_catalog."default" NOT NULL,
    trad_id text COLLATE pg_catalog."default" NOT NULL,
    trad_issrreq text COLLATE pg_catalog."default" NOT NULL,
    trad_frsttraddt timestamp without time zone NOT NULL,
    trad_termntndt timestamp without time zone NOT NULL,
    tech_rlvntcmptntauthrty text COLLATE pg_catalog."default" NOT NULL,
    undlyng_type text COLLATE pg_catalog."default" NOT NULL,
    undlyng_subtype text COLLATE pg_catalog."default" NOT NULL,
    undlyng_isins text COLLATE pg_catalog."default" NOT NULL
)

TABLESPACE pg_default;

ALTER TABLE integration.staged_instrument
    OWNER to postgres;
	
-- Table: mobi_ss.dim_instrument

-- DROP TABLE IF EXISTS mobi_ss.dim_instrument if exists;

CREATE TABLE mobi_ss.dim_instrument
(
    instrm_key serial primary key,
    instrm_id text COLLATE pg_catalog."default" NOT NULL,
    industry_nm text COLLATE pg_catalog."default" NOT NULL,
    type text COLLATE pg_catalog."default" NOT NULL,
    instrm_type text COLLATE pg_catalog."default" NOT NULL,
    instrm_fullnm text COLLATE pg_catalog."default" NOT NULL,
    instrm_shrtnm text COLLATE pg_catalog."default" NOT NULL,
    instrm_clssfctntp text COLLATE pg_catalog."default" NOT NULL,
    instrm_ntnlccy text COLLATE pg_catalog."default" NOT NULL,
    instrm_cmmdtyderivind text COLLATE pg_catalog."default" NOT NULL,
    issr text COLLATE pg_catalog."default" NOT NULL,
    trad_id text COLLATE pg_catalog."default" NOT NULL,
    trad_issrreq text COLLATE pg_catalog."default" NOT NULL,
    trad_frsttraddt timestamp without time zone NOT NULL,
    trad_termntndt timestamp without time zone NOT NULL,
    tech_rlvntcmptntauthrty text COLLATE pg_catalog."default" NOT NULL,
    undlyng_type text COLLATE pg_catalog."default" NOT NULL,
    undlyng_subtype text COLLATE pg_catalog."default" NOT NULL,
    undlyng_isins text COLLATE pg_catalog."default" NOT NULL,
    basket_array text[] COLLATE pg_catalog."default",
	created_date timestamp without time zone NOT NULL, 
	last_modified_date timestamp without time zone NOT NULL,
	current_active integer default 1
)

TABLESPACE pg_default;

ALTER TABLE mobi_ss.dim_instrument
    OWNER to postgres;

-- Table: mobi_ss.dim_date

-- DROP TABLE IF EXISTS mobi_ss.dim_date if exists;

create table mobi_ss.dim_date (
date_key serial Primary Key,
date_desc varchar(60),
full_date date,
sql_date varchar(10),
month_number integer,
month_name varchar(30),
year integer
)

TABLESPACE pg_default;

ALTER TABLE mobi_ss.dim_date
    OWNER to postgres;
	
/*	
select 
date_trunc('year', trad_frsttraddt) y,
date_trunc('year', trad_termntndt) z,
	COUNT (instrm_id) instrm
from integration.staged_instrument
group by y,z order by z desc,y;

select 
date_trunc('year', trad_termntndt) y,
	COUNT (instrm_id) instrm
from integration.staged_instrument
group by y order by y;

select * from integration.staged_instrument
where instrm_id in (
	select instrm_id from (
select instrm_id ,count(1)
from integration.staged_instrument
group by instrm_id having count(1) > 1) y
	) order by instrm_id;
	
select * from integration.staged_instrument where instrm_id = 'CH0420776905';
*/

INSERT INTO mobi_ss.dim_date(date_desc
							 , full_date
							 , sql_date 
							 , month_number
							 , month_name
							 , year)
SELECT 
to_char(days.d, 'FMMonth DD, YYYY'), 
days.d::DATE, 
to_char(days.d, 'YYYY-MM-DD'),
to_char(days.d, 'MM')::integer, 
to_char(days.d, 'FMMonth'), 
to_char(days.d, 'YYYY')::integer 
from (
    SELECT generate_series(
        ('2016-01-01')::date, -- 'start' date
        ('2033-12-31')::date, -- 'end' date
        interval '1 day'  -- one for each day between the start and day
        )) as days(d);


DROP VIEW IF EXISTS mobi_ss.closingprice_date;

CREATE VIEW mobi_ss.closingprice_date AS
SELECT date_key
       , date_desc
		, full_date
		, sql_date 
		, month_number
		, month_name
		, year
FROM  mobi_ss.dim_date;	

ALTER VIEW mobi_ss.closingprice_date
    OWNER to postgres;	

select * from mobi_ss.closingprice_date 
where full_date >= '2021-01-01' and full_date < '2021-01-14'	
		
/*
select 
date_trunc('year', trad_frsttraddt) y,
date_trunc('year', trad_termntndt) z,
	COUNT (instrm_id) instrm
from integration.staged_instrument
group by y,z order by z desc,y;

select 
date_trunc('year', trad_termntndt) y,
	COUNT (instrm_id) instrm
from integration.staged_instrument
group by y order by y;

create table mobi_ss.dim_date (
date_key serial Primary Key,
date_desc varchar(60),
full_date date,
sql_date varchar(10),
month_number integer,
month_name varchar(30),
year integer
)

TABLESPACE pg_default;

ALTER TABLE mobi_ss.dim_date
    OWNER to postgres;
	
select * from mobi_ss.dim_date;

INSERT INTO mobi_ss.dim_date(date_desc
							 , full_date
							 , sql_date 
							 , month_number
							 , month_name
							 , year)
SELECT 
to_char(days.d, 'FMMonth DD, YYYY'), 
days.d::DATE, 
to_char(days.d, 'YYYY-MM-DD'),
to_char(days.d, 'MM')::integer, 
to_char(days.d, 'FMMonth'), 
to_char(days.d, 'YYYY')::integer 
from (
    SELECT generate_series(
        ('2016-01-01')::date, -- 'start' date
        ('2033-12-31')::date, -- 'end' date
        interval '1 day'  -- one for each day between the start and day
        )) as days(d);

INSERT INTO mobi_ss.dim_date(date_desc
							 , full_date
							 , sql_date 
							 , month_number
							 , month_name
							 , year)
SELECT 
to_char(days.d, 'FMMonth DD, YYYY'), 
days.d::DATE, 
to_char(days.d, 'YYYY-MM-DD'),
to_char(days.d, 'MM')::integer, 
to_char(days.d, 'FMMonth'), 
to_char(days.d, 'YYYY')::integer 
from (
    SELECT generate_series(
        ('9999-01-01')::date, -- 'start' date
        ('9999-12-31')::date, -- 'end' date
        interval '1 day'  -- one for each day between the start and day
        )) as days(d);
*/
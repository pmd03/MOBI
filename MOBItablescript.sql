
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
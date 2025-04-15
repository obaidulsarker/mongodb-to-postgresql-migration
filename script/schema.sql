CREATE TABLE IF NOT EXISTS etl.etl_snapshoot
(
    trg_table_name character varying(200) COLLATE pg_catalog."default" NOT NULL,
    src_index_name character varying(200) COLLATE pg_catalog."default" NOT NULL,
    src_pk_col_name character varying(100) COLLATE pg_catalog."default",
    src_timestamp_col_name character varying(100) COLLATE pg_catalog."default",
    last_sync_timestamp timestamp without time zone,
    is_active boolean DEFAULT true,
    CONSTRAINT etl_snapshoot_pkey PRIMARY KEY (trg_table_name)
);


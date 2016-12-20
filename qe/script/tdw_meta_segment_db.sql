\connect seg_1

--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: bucket_cols; Type: TABLE; Schema: public; Owner: tdwmeta; Tablespace: 
--

CREATE TABLE bucket_cols (
    tbl_id bigint NOT NULL,
    bucket_col_name character varying NOT NULL,
    col_index integer NOT NULL
);


ALTER TABLE public.bucket_cols OWNER TO tdwmeta;

--
-- Name: columns; Type: TABLE; Schema: public; Owner: tdwmeta; Tablespace: 
--

CREATE TABLE columns (
    column_index bigint NOT NULL,
    tbl_id bigint NOT NULL,
    column_len bigint DEFAULT 0,
    column_name character varying NOT NULL,
    type_name character varying NOT NULL,
    comment character varying
);


ALTER TABLE public.columns OWNER TO tdwmeta;

--
-- Name: dbs; Type: TABLE; Schema: public; Owner: tdwmeta; Tablespace: 
--

CREATE TABLE dbs (
    name character varying NOT NULL,
    hdfs_schema character varying,
    description character varying,
    owner character varying
);


ALTER TABLE public.dbs OWNER TO tdwmeta;


--
-- Name: partitions; Type: TABLE; Schema: public; Owner: tdwmeta; Tablespace: 
--

CREATE TABLE partitions (
    level integer DEFAULT 0,
    tbl_id bigint NOT NULL,
    create_time timestamp without time zone DEFAULT now(),
    part_name character varying NOT NULL,
    part_values character varying[]
);


ALTER TABLE public.partitions OWNER TO tdwmeta;

--
-- Name: sort_cols; Type: TABLE; Schema: public; Owner: tdwmeta; Tablespace: 
--

CREATE TABLE sort_cols (
    tbl_id bigint NOT NULL,
    sort_column_name character varying NOT NULL,
    sort_order integer NOT NULL,
    col_index integer NOT NULL
);


ALTER TABLE public.sort_cols OWNER TO tdwmeta;

--
-- Name: table_params; Type: TABLE; Schema: public; Owner: tdwmeta; Tablespace: 
--

CREATE TABLE table_params (
    tbl_id bigint NOT NULL,
    param_type character varying NOT NULL,
    param_key character varying NOT NULL,
    param_value character varying NOT NULL
);


ALTER TABLE public.table_params OWNER TO tdwmeta;

--
-- Name: tbls; Type: TABLE; Schema: public; Owner: tdwmeta; Tablespace: 
--

CREATE TABLE tbls (
    tbl_id bigint NOT NULL,
    create_time timestamp without time zone DEFAULT now() NOT NULL,
    is_compressed boolean DEFAULT false NOT NULL,
    retention bigint DEFAULT 0,
    tbl_type character varying NOT NULL,
    db_name character varying NOT NULL,
    tbl_name character varying NOT NULL,
    tbl_owner character varying,
    tbl_format character varying,
    pri_part_type character varying,
    sub_part_type character varying,
    pri_part_key character varying,
    sub_part_key character varying,
    input_format character varying,
    output_format character varying,
    serde_name character varying,
    serde_lib character varying,
    tbl_location character varying,
    tbl_comment character varying
);


ALTER TABLE public.tbls OWNER TO tdwmeta;

--
-- Name: tdwview; Type: TABLE; Schema: public; Owner: tdwmeta; Tablespace: 
--

CREATE TABLE tdwview (
    tbl_id bigint NOT NULL,
    view_original_text character varying NOT NULL,
    view_expanded_text character varying NOT NULL,
    vtables character varying NOT NULL
);


ALTER TABLE public.tdwview OWNER TO tdwmeta;

--
-- Name: bucket_cols_tbl_id_col_index_key; Type: CONSTRAINT; Schema: public; Owner: tdwmeta; Tablespace: 
--

ALTER TABLE ONLY bucket_cols
    ADD CONSTRAINT bucket_cols_tbl_id_col_index_key UNIQUE (tbl_id, col_index);


--
-- Name: columns_tbl_id_column_name_key; Type: CONSTRAINT; Schema: public; Owner: tdwmeta; Tablespace: 
--

ALTER TABLE ONLY columns
    ADD CONSTRAINT columns_tbl_id_column_name_key UNIQUE (tbl_id, column_name);


--
-- Name: dbs_pkey; Type: CONSTRAINT; Schema: public; Owner: tdwmeta; Tablespace: 
--

ALTER TABLE ONLY dbs
    ADD CONSTRAINT dbs_pkey PRIMARY KEY (name);


--
-- Name: partitions_tbl_id_level_part_name_key; Type: CONSTRAINT; Schema: public; Owner: tdwmeta; Tablespace: 
--

ALTER TABLE ONLY partitions
    ADD CONSTRAINT partitions_tbl_id_level_part_name_key UNIQUE (tbl_id, level, part_name);


--
-- Name: sort_cols_tbl_id_col_index_key; Type: CONSTRAINT; Schema: public; Owner: tdwmeta; Tablespace: 
--

ALTER TABLE ONLY sort_cols
    ADD CONSTRAINT sort_cols_tbl_id_col_index_key UNIQUE (tbl_id, col_index);


--
-- Name: table_params_tbl_id_param_type_param_key_key; Type: CONSTRAINT; Schema: public; Owner: tdwmeta; Tablespace: 
--

ALTER TABLE ONLY table_params
    ADD CONSTRAINT table_params_tbl_id_param_type_param_key_key UNIQUE (tbl_id, param_type, param_key);


--
-- Name: tbls_pkey; Type: CONSTRAINT; Schema: public; Owner: tdwmeta; Tablespace: 
--

ALTER TABLE ONLY tbls
    ADD CONSTRAINT tbls_pkey PRIMARY KEY (tbl_id);


--
-- Name: tbls_tbl_name_db_name_key; Type: CONSTRAINT; Schema: public; Owner: tdwmeta; Tablespace: 
--

ALTER TABLE ONLY tbls
    ADD CONSTRAINT tbls_tbl_name_db_name_key UNIQUE (tbl_name, db_name);



--
-- Name: bucket_cols_tbl_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: tdwmeta
--

ALTER TABLE ONLY bucket_cols
    ADD CONSTRAINT bucket_cols_tbl_id_fkey FOREIGN KEY (tbl_id) REFERENCES tbls(tbl_id) ON DELETE CASCADE;


--
-- Name: columns_tbl_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: tdwmeta
--

ALTER TABLE ONLY columns
    ADD CONSTRAINT columns_tbl_id_fkey FOREIGN KEY (tbl_id) REFERENCES tbls(tbl_id) ON DELETE CASCADE;


--
-- Name: partitions_tbl_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: tdwmeta
--

ALTER TABLE ONLY partitions
    ADD CONSTRAINT partitions_tbl_id_fkey FOREIGN KEY (tbl_id) REFERENCES tbls(tbl_id) ON DELETE CASCADE;


--
-- Name: sort_cols_tbl_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: tdwmeta
--

ALTER TABLE ONLY sort_cols
    ADD CONSTRAINT sort_cols_tbl_id_fkey FOREIGN KEY (tbl_id) REFERENCES tbls(tbl_id) ON DELETE CASCADE;


--
-- Name: table_params_tbl_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: tdwmeta
--

ALTER TABLE ONLY table_params
    ADD CONSTRAINT table_params_tbl_id_fkey FOREIGN KEY (tbl_id) REFERENCES tbls(tbl_id) ON DELETE CASCADE;


--
-- Name: tbls_db_name_fkey; Type: FK CONSTRAINT; Schema: public; Owner: tdwmeta
--

ALTER TABLE ONLY tbls
    ADD CONSTRAINT tbls_db_name_fkey FOREIGN KEY (db_name) REFERENCES dbs(name) ON DELETE CASCADE;


--
-- Name: tdwview_tbl_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: tdwmeta
--

ALTER TABLE ONLY tdwview
    ADD CONSTRAINT tdwview_tbl_id_fkey FOREIGN KEY (tbl_id) REFERENCES tbls(tbl_id) ON DELETE CASCADE;

REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

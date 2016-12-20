\connect tdw_query_info

--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

CREATE SCHEMA tdw;


ALTER SCHEMA tdw OWNER TO tdw;

alter role tdw in database tdw_query_info set search_path='tdw';

set search_path='tdw';


--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


CREATE TABLE tdw_query_info_new (
    mrnum integer,
    finishtime timestamp without time zone,
    queryid character varying(128) NOT NULL,
    querystring character varying DEFAULT NULL::character varying,
    starttime timestamp without time zone DEFAULT now() NOT NULL,
    username character varying(128) DEFAULT NULL::character varying,
    ip character varying(256) DEFAULT NULL::character varying,
    taskid character varying(256) DEFAULT NULL::character varying,
	port character varying(128),
	clientip character varying(256),
	dbname character varying(256)
);

ALTER TABLE ONLY tdw_query_info_new
    ADD CONSTRAINT tdw_query_info_new_pkey PRIMARY KEY (queryid);

ALTER TABLE tdw.tdw_query_info_new OWNER TO tdw;


CREATE TABLE tdw_ddl_query_info (
    starttime timestamp without time zone DEFAULT now() NOT NULL,
    finishtime timestamp without time zone,
    queryid character varying NOT NULL,
    querystring character varying,
    username character varying,
    dbname character varying,
    ip character varying,
    queryresult boolean,
    taskid character varying
);

--
-- Name: tdw_ddl_query_info_new2_queryid_idx; Type: INDEX; Schema: tdw; Owner: tdw; Tablespace: 
--

CREATE INDEX tdw_ddl_query_info_new2_queryid_idx ON tdw_ddl_query_info USING btree (queryid);


--
-- Name: tdw_ddl_query_info_new2_starttime_idx; Type: INDEX; Schema: tdw; Owner: tdw; Tablespace: 
--

CREATE INDEX tdw_ddl_query_info_new2_starttime_idx ON tdw_ddl_query_info USING btree (starttime);


--
-- Name: tdw_ddl_query_info_new2_taskid_idx; Type: INDEX; Schema: tdw; Owner: tdw; Tablespace: 
--

CREATE INDEX tdw_ddl_query_info_new2_taskid_idx ON tdw_ddl_query_info USING btree (taskid);


ALTER TABLE tdw.tdw_ddl_query_info OWNER TO tdw;


CREATE TABLE tdw_insert_info (
    queryid character varying,
    desttable character varying,
    successnum bigint,
    rejectnum bigint,
    ismultiinsert boolean,
    inserttime timestamp without time zone DEFAULT now()
);


ALTER TABLE tdw.tdw_insert_info OWNER TO tdw;

CREATE TABLE tdw_move_info (
    log_time timestamp without time zone DEFAULT now() NOT NULL,
    queryid character varying(128) NOT NULL,
    srcdir character varying(4000) NOT NULL,
    destdir character varying(4000) NOT NULL,
    dbname character varying(100),
    tbname character varying(100),
    taskid character varying(256)
);

ALTER TABLE tdw.tdw_move_info OWNER TO tdw;

CREATE TABLE tdw_query_stat_new (
    mapnum integer,
    reducenum integer,
    currmrfinishtime timestamp without time zone,
    currmrid character varying(128) DEFAULT NULL::character varying NOT NULL,
    currmrindex integer NOT NULL,
    currmrstarttime timestamp without time zone DEFAULT now() NOT NULL,
    queryid character varying(128) DEFAULT NULL::character varying NOT NULL,
    jtip character varying
);


ALTER TABLE tdw.tdw_query_stat_new OWNER TO tdw;

--
-- Name: tdw_query_stat_new_currmrid_idx; Type: INDEX; Schema: tdw; Owner: tdw; Tablespace: 
--

CREATE INDEX tdw_query_stat_new_currmrid_idx ON tdw_query_stat_new USING btree (currmrid);


--
-- Name: tdw_query_stat_new_currmrstarttime_idx; Type: INDEX; Schema: tdw; Owner: tdw; Tablespace: 
--

CREATE INDEX tdw_query_stat_new_currmrstarttime_idx ON tdw_query_stat_new USING btree (currmrstarttime);


CREATE TABLE tdw_query_error_info_new
(
  queryid character varying(128) NOT NULL,
  taskid character varying(256) DEFAULT NULL::character varying,
  errortime timestamp without time zone NOT NULL DEFAULT now(),
  ip character varying(256) DEFAULT NULL::character varying,
  port character varying(128) DEFAULT NULL::character varying,
  clientip character varying(256) DEFAULT NULL::character varying,
  errorstring character varying DEFAULT NULL::character varying,
  errorid character varying(256) DEFAULT NULL::character varying,
  CONSTRAINT tdw_query_error_info_new_pkey PRIMARY KEY (queryid)
);

ALTER TABLE tdw.tdw_query_error_info_new OWNER TO tdw;

CREATE INDEX tdw_query_error_info_new_errortime_idx
  ON tdw_query_error_info_new
  USING btree
  (errortime);

--
-- PostgreSQL database dump complete
--

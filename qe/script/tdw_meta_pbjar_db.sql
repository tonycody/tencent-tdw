\connect pbjar

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
-- Name: pb_proto_jar; Type: TABLE; Schema: public; Owner: tdwmeta; Tablespace: 
--

CREATE TABLE pb_proto_jar (
    db_name character varying NOT NULL,
    tbl_name character varying NOT NULL,
    proto_name character varying,
    proto_file bytea,
    jar_name character varying,
    jar_file bytea,
    user_name character varying,
    modified_time timestamp without time zone NOT NULL,
	protobuf_version character varying default '2.3.0'::character varying
);


ALTER TABLE public.pb_proto_jar OWNER TO tdwmeta;

--
-- Name: pb_proto_jar_pkey; Type: CONSTRAINT; Schema: public; Owner: tdwmeta; Tablespace: 
--

ALTER TABLE ONLY pb_proto_jar
    ADD CONSTRAINT pb_proto_jar_pkey PRIMARY KEY (db_name, tbl_name, modified_time);


REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

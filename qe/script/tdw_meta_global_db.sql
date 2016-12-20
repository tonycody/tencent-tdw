\connect global

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
-- Name: dbpriv; Type: TABLE; Schema: public; Owner: tdwmeta; Tablespace: 
--

CREATE TABLE dbpriv (
    alter_priv boolean DEFAULT false NOT NULL,
    create_priv boolean DEFAULT false NOT NULL,
    createview_priv boolean DEFAULT false NOT NULL,
    delete_priv boolean DEFAULT false NOT NULL,
    drop_priv boolean DEFAULT false NOT NULL,
    index_priv boolean DEFAULT false NOT NULL,
    insert_priv boolean DEFAULT false NOT NULL,
    select_priv boolean DEFAULT false NOT NULL,
    showview_priv boolean DEFAULT false NOT NULL,
    update_priv boolean DEFAULT false NOT NULL,
    user_name character varying NOT NULL,
    db_name character varying NOT NULL,
    out_of_date_time timestamp without time zone,
    start_date_time timestamp without time zone DEFAULT now(),
    CONSTRAINT must_lower CHECK (((lower((db_name)::text) = (db_name)::text) AND (lower((user_name)::text) = (user_name)::text)))
);


ALTER TABLE public.dbpriv OWNER TO tdwmeta;

--
-- Name: dbsensitivity; Type: TABLE; Schema: public; Owner: tdwmeta; Tablespace: 
--

CREATE TABLE dbsensitivity (
    db_name character varying NOT NULL,
    sensitivity integer DEFAULT 0,
    create_time timestamp without time zone,
    update_time timestamp without time zone,
    detail character varying,
    CONSTRAINT must_lower CHECK ((lower((db_name)::text) = (db_name)::text))
);


ALTER TABLE public.dbsensitivity OWNER TO tdwmeta;

--
-- Name: router; Type: TABLE; Schema: public; Owner: tdwmeta; Tablespace: 
--

CREATE TABLE router (
    db_name character varying NOT NULL,
    seg_addr character varying NOT NULL,
    secondary_seg_addr character varying,
    is_db_split boolean DEFAULT false,
    hashcode integer NOT NULL,
    describe character varying,
    owner character varying
);


ALTER TABLE public.router OWNER TO tdwmeta;

--
-- Name: seg_split; Type: TABLE; Schema: public; Owner: tdwmeta; Tablespace: 
--

CREATE TABLE seg_split (
    seg_addr character varying NOT NULL,
    seg_accept_range int4range
);


ALTER TABLE public.seg_split OWNER TO tdwmeta;

insert into seg_split values('jdbc:postgresql://127.0.0.1:5432/seg_1','[0,10000)');

--
-- Name: tblpriv; Type: TABLE; Schema: public; Owner: tdwmeta; Tablespace: 
--

CREATE TABLE tblpriv (
    alter_priv boolean DEFAULT false NOT NULL,
    create_priv boolean DEFAULT false NOT NULL,
    delete_priv boolean DEFAULT false NOT NULL,
    drop_priv boolean DEFAULT false NOT NULL,
    index_priv boolean DEFAULT false NOT NULL,
    insert_priv boolean DEFAULT false NOT NULL,
    select_priv boolean DEFAULT false NOT NULL,
    update_priv boolean DEFAULT false NOT NULL,
    user_name character varying NOT NULL,
    db_name character varying NOT NULL,
    tbl_name character varying NOT NULL,
    out_of_date_time timestamp without time zone,
    start_date_time timestamp without time zone DEFAULT now(),
    CONSTRAINT must_lower CHECK ((((lower((db_name)::text) = (db_name)::text) AND (lower((user_name)::text) = (user_name)::text)) AND (lower((tbl_name)::text) = (tbl_name)::text)))
);


ALTER TABLE public.tblpriv OWNER TO tdwmeta;

--
-- Name: tblsensitivity; Type: TABLE; Schema: public; Owner: tdwmeta; Tablespace: 
--

CREATE TABLE tblsensitivity (
    db_name character varying NOT NULL,
    tbl_name character varying NOT NULL,
    sensitivity integer,
    create_time timestamp without time zone,
    update_time timestamp without time zone,
    detail character varying,
    CONSTRAINT must_lower CHECK (((lower((tbl_name)::text) = (tbl_name)::text) AND (lower((db_name)::text) = (db_name)::text)))
);


ALTER TABLE public.tblsensitivity OWNER TO tdwmeta;

--
-- Name: tdw_badpbfile_skip_log; Type: TABLE; Schema: public; Owner: tdwmeta; Tablespace: 
--

CREATE TABLE tdw_badpbfile_skip_log (
    queryid character varying NOT NULL,
    mrid character varying NOT NULL,
    badfilenum bigint,
    logtime timestamp without time zone DEFAULT now()
);


ALTER TABLE public.tdw_badpbfile_skip_log OWNER TO tdwmeta;

--
-- Name: tdw_db_router; Type: VIEW; Schema: public; Owner: tdwmeta
--

CREATE VIEW tdw_db_router AS
    SELECT "substring"((router.seg_addr)::text, 'jdbc:postgresql://#"_{10,16}#":_{4,5}/%'::text, '#'::text) AS host, "substring"((router.seg_addr)::text, 'jdbc:postgresql://%:#"_{4,5}#"/%'::text, '#'::text) AS port, "substring"((router.seg_addr)::text, 'jdbc:postgresql://%/#"%#"'::text, '#'::text) AS meta_db_name, router.db_name AS tdw_db_name, router.hashcode FROM router;


ALTER TABLE public.tdw_db_router OWNER TO tdwmeta;

--
-- Name: tdwrole; Type: TABLE; Schema: public; Owner: tdwmeta; Tablespace: 
--

CREATE TABLE tdwrole (
    alter_priv boolean DEFAULT false NOT NULL,
    create_priv boolean DEFAULT false NOT NULL,
    createview_priv boolean DEFAULT false NOT NULL,
    dba_priv boolean DEFAULT false NOT NULL,
    delete_priv boolean DEFAULT false NOT NULL,
    drop_priv boolean DEFAULT false NOT NULL,
    index_priv boolean DEFAULT false NOT NULL,
    insert_priv boolean DEFAULT false NOT NULL,
    select_priv boolean DEFAULT false NOT NULL,
    showview_priv boolean DEFAULT false NOT NULL,
    update_priv boolean DEFAULT false NOT NULL,
    map_num bigint DEFAULT 300 NOT NULL,
    reduce_num bigint DEFAULT 300 NOT NULL,
    create_time timestamp without time zone DEFAULT now() NOT NULL,
    role_name character varying NOT NULL,
    product_name character varying,
    home_dir character varying,
    out_of_date_time timestamp without time zone,
    CONSTRAINT must_lower CHECK ((lower((role_name)::text) = (role_name)::text))
);


ALTER TABLE public.tdwrole OWNER TO tdwmeta;

--
-- Name: tdwuser; Type: TABLE; Schema: public; Owner: tdwmeta; Tablespace: 
--

CREATE TABLE tdwuser (
    alter_priv boolean DEFAULT false NOT NULL,
    create_priv boolean DEFAULT false NOT NULL,
    createview_priv boolean DEFAULT false NOT NULL,
    dba_priv boolean DEFAULT false NOT NULL,
    delete_priv boolean DEFAULT false NOT NULL,
    drop_priv boolean DEFAULT false NOT NULL,
    index_priv boolean DEFAULT false NOT NULL,
    insert_priv boolean DEFAULT false NOT NULL,
    select_priv boolean DEFAULT false NOT NULL,
    showview_priv boolean DEFAULT false NOT NULL,
    update_priv boolean DEFAULT false NOT NULL,
    create_time timestamp without time zone DEFAULT now() NOT NULL,
    expire_time timestamp without time zone,
    timetolive bigint DEFAULT (-1) NOT NULL,
    user_name character varying NOT NULL,
    group_name character varying DEFAULT 'default'::character varying NOT NULL,
    passwd character varying NOT NULL,
    out_of_date_time timestamp without time zone,
    CONSTRAINT must_lower CHECK ((lower((user_name)::text) = (user_name)::text))
);


ALTER TABLE public.tdwuser OWNER TO tdwmeta;

--
-- Name: tdwuserrole; Type: TABLE; Schema: public; Owner: tdwmeta; Tablespace: 
--

CREATE TABLE tdwuserrole (
    user_name character varying NOT NULL,
    role_name character varying NOT NULL,
    user_level character varying,
    CONSTRAINT must_lower CHECK (((lower((user_name)::text) = (user_name)::text) AND (lower((role_name)::text) = (role_name)::text)))
);


ALTER TABLE public.tdwuserrole OWNER TO tdwmeta;

--
-- Name: usergroup; Type: TABLE; Schema: public; Owner: tdwmeta; Tablespace: 
--

CREATE TABLE usergroup (
    group_name character varying NOT NULL,
    creator character varying NOT NULL
);


ALTER TABLE public.usergroup OWNER TO tdwmeta;

--
-- Name: dbpriv_user_name_db_name_key; Type: CONSTRAINT; Schema: public; Owner: tdwmeta; Tablespace: 
--

ALTER TABLE ONLY dbpriv
    ADD CONSTRAINT dbpriv_user_name_db_name_key UNIQUE (user_name, db_name);

--
-- Name: dbsensitivity_pkey; Type: CONSTRAINT; Schema: public; Owner: tdwmeta; Tablespace: 
--
ALTER TABLE ONLY dbsensitivity
    ADD CONSTRAINT dbsensitivity_pkey PRIMARY KEY (db_name);

--
-- Name: dw_badfile_skip_log_pkey; Type: CONSTRAINT; Schema: public; Owner: tdwmeta; Tablespace: 
--
ALTER TABLE ONLY tdw_badpbfile_skip_log
    ADD CONSTRAINT dw_badfile_skip_log_pkey PRIMARY KEY (queryid, mrid);


--
-- Name: router_pkey; Type: CONSTRAINT; Schema: public; Owner: tdwmeta; Tablespace: 
--

ALTER TABLE ONLY router
    ADD CONSTRAINT router_pkey PRIMARY KEY (db_name);

--
-- Name: seg_split_pkey; Type: CONSTRAINT; Schema: public; Owner: tdwmeta; Tablespace: 
--

ALTER TABLE ONLY seg_split
    ADD CONSTRAINT seg_split_pkey PRIMARY KEY (seg_addr);

--
-- Name: tblpriv_user_name_db_name_tbl_name_key; Type: CONSTRAINT; Schema: public; Owner: tdwmeta; Tablespace: 
--

ALTER TABLE ONLY tblpriv
    ADD CONSTRAINT tblpriv_user_name_db_name_tbl_name_key UNIQUE (user_name, db_name, tbl_name);

--
-- Name: tblsensitivity_pkey1; Type: CONSTRAINT; Schema: public; Owner: tdwmeta; Tablespace: 
--
ALTER TABLE ONLY tblsensitivity
    ADD CONSTRAINT tblsensitivity_pkey1 PRIMARY KEY (tbl_name, db_name);

--
-- Name: tdwrole_pkey; Type: CONSTRAINT; Schema: public; Owner: tdwmeta; Tablespace: 
--

ALTER TABLE ONLY tdwrole
    ADD CONSTRAINT tdwrole_pkey PRIMARY KEY (role_name);


--
-- Name: tdwuser_pkey; Type: CONSTRAINT; Schema: public; Owner: tdwmeta; Tablespace: 
--

ALTER TABLE ONLY tdwuser
    ADD CONSTRAINT tdwuser_pkey PRIMARY KEY (user_name);


--
-- Name: usergroup_pkey; Type: CONSTRAINT; Schema: public; Owner: tdwmeta; Tablespace: 
--

ALTER TABLE ONLY usergroup
    ADD CONSTRAINT usergroup_pkey PRIMARY KEY (group_name);

--
-- Name: tdwuserrole_role_name_fkey; Type: FK CONSTRAINT; Schema: public; Owner: tdwmeta
--

ALTER TABLE ONLY tdwuserrole
    ADD CONSTRAINT tdwuserrole_role_name_fkey FOREIGN KEY (role_name) REFERENCES tdwrole(role_name) ON DELETE CASCADE;


--
-- Name: tdwuserrole_user_name_fkey; Type: FK CONSTRAINT; Schema: public; Owner: tdwmeta
--

ALTER TABLE ONLY tdwuserrole
    ADD CONSTRAINT tdwuserrole_user_name_fkey FOREIGN KEY (user_name) REFERENCES tdwuser(user_name) ON DELETE CASCADE;


REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

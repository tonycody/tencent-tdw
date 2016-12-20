--
-- PostgreSQL database cluster dump
--

SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;

--
-- Roles
--

--NOTE: change the password in production evn 
CREATE ROLE tdw;
ALTER ROLE tdw WITH NOSUPERUSER INHERIT NOCREATEROLE NOCREATEDB LOGIN NOREPLICATION PASSWORD 'md56d713efc51a858219e00d60efacf6031';

--NOTE: change the password in production evn 
CREATE ROLE tdwmeta;
ALTER ROLE tdwmeta WITH NOSUPERUSER INHERIT NOCREATEROLE NOCREATEDB LOGIN NOREPLICATION PASSWORD 'md58d7da909fc522bf51b3f21cb64193b06';

--for pgdata test,add a pg user 'root',with password set to 'tdwroot'
--NOTE: change the password in production evn 
CREATE ROLE root;
ALTER ROLE root WITH NOSUPERUSER INHERIT NOCREATEROLE NOCREATEDB LOGIN NOREPLICATION PASSWORD 'md5c42560909c2dd6122db208225fff54fe';

--
-- Database creation
--

CREATE DATABASE global WITH TEMPLATE = template0 OWNER = tdwmeta;
CREATE DATABASE pbjar WITH TEMPLATE = template0 OWNER = tdwmeta;
CREATE DATABASE seg_1 WITH TEMPLATE = template0 OWNER = tdwmeta;
CREATE DATABASE tdw_query_info WITH TEMPLATE = template0 OWNER = tdw;

\ir tdw_meta_global_db.sql

\ir tdw_meta_segment_db.sql

\ir tdw_meta_pbjar_db.sql

\ir tdw_meta_query_info_db.sql

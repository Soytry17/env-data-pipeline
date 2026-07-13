-- create database
CREATE DATABASE weather_database;

-- create a dedicated user
CREATE USER pipeline_user_01 WITH PASSWORD 'try_pipeline';

-- grant access
GRANT ALL PRIVILEGES ON DATABASE weather_database TO pipeline_user_01;

-- create the three layer schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- grant all privileges to your pipeline user on each schema
GRANT ALL ON SCHEMA bronze TO pipeline_user_01;
GRANT ALL ON SCHEMA silver TO pipeline_user_01;
GRANT ALL ON SCHEMA gold   TO pipeline_user_01;

-- allow pipeline_user to create tables inside each schema
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze
    GRANT ALL ON TABLES TO pipeline_user_01;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver
    GRANT ALL ON TABLES TO pipeline_user_01;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold
    GRANT ALL ON TABLES TO pipeline_user_01;

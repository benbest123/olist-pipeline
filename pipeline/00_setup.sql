-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS IDENTIFIER(:catalog);
USE CATALOG IDENTIFIER(:catalog);

CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Raw data landed as-is from source CSVs';

CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Cleaned, conformed and normalised (3NF) data';

CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Star schema: dimensions and facts, ready for analytics';

CREATE VOLUME IF NOT EXISTS bronze.raw
COMMENT 'Landing zone for raw Olist CSV files before ingestion into bronze tables';

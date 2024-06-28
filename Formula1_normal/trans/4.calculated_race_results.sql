-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

CREATE TABLE f1_presentation.calculated_race_results
USING parquet
AS
SELECT race.race_year,
       constructors.name AS team_name,
       drivers.name AS driver_name,
       results.position,
       results.points,
       11-results.position AS calculated_points
   FROM results
   JOIN f1_processed.drivers ON (results.driver_id==drivers.driver_id)
   JOIN f1_processed.constructors ON (results.constructor_id==constructors.constructor_id)
   join f1_processed.race ON (results.race_id==race.race_id)
   WHERE results.position <=10

-- COMMAND ----------


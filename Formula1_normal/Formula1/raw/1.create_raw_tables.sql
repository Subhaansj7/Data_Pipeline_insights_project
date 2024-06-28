-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###Create circuits tabel

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(circuitID INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lnh DOUBLE,
alt INT,
url STRING
)
USING csv
OPTIONS (path"/mnt/formula1dlil/raw/circuits.csv", header=true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date STRING,
time STRING,
url STRING
)
USING csv 
OPTIONS(path"/mnt/formula1dlil/raw/races.csv",header=true)


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create tables for Json files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create constructors table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING
)
USING json
OPTIONS(path"/mnt/formula1dlil/raw/constructors.json",header=true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Drivers table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename :STRING ,surname:STRING>,
dob DATE,
nationality STRING,
url STRING
)
USING json
OPTIONS(path"/mnt/formula1dlil/raw/drivers.json",header=true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create Results table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
grid INT,
position INT,
postionText STRING,
postionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING
)
USING json
OPTIONS(path"/mnt/formula1dlil/raw/results.json",header=true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create Pit stops tabel

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING
)
USING json
OPTIONS(path"/mnt/formula1dlil/raw/pit_stops.json",multiline=true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CREATE TABLE FOR LIST OF FILES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###create lap times table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS(path"/mnt/formula1dlil/raw/lap_times")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create qualifying table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(qualifyId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING
)
USING json
OPTIONS(path"/mnt/formula1dlil/raw/qualifying",multiline=true)
# Databricks Notebook: Delta Lake
# Converted from .dbc to a readable Python notebook source

# ===== Cell 1 =====
%run "/DatabricksMasterclass/Tutorial"

# ===== Cell 2 =====
%md
**OPTIMIZE**

# ===== Cell 3 =====
df_sales.write.format('parquet')\
        .mode('append')\
        .option('path','abfss://destination@datalakeansh.dfs.core.windows.net/sales')\
        .save()

# ===== Cell 4 =====
%md
**VACUUM RETAIN 0 HRS**

# ===== Cell 6 =====
df = spark.readStream.format('cloudFiles')\
        .option('cloudFiles.format','parquet')\
        .option('cloudFiles.schemaLocation','abfss://aldestination@datalakeansh.dfs.core.windows.net/checkpoint')\
        .load('abfss://alsource@datalakeansh.dfs.core.windows.net')

# ===== Cell 7 =====
%md
# DELTA LAKE

# ===== Cell 8 =====
%sql
INSERT INTO salesDB.exttable 
VALUES
(1,'aa',30),
(2,'bb',33),
(3,'cc',35),
(4,'DD',40)

# ===== Cell 9 =====
%md
**INSERT**

# ===== Cell 10 =====
%sql
select * from salesdb.exttable

# ===== Cell 11 =====
%sql
select * from salesdb.exttable

# ===== Cell 12 =====
%md
**Database**

# ===== Cell 13 =====
%md
**Managed Table**

# ===== Cell 14 =====
%sql
INSERT INTO salesDB.mantable 
VALUES
(1,'aa',30),
(2,'bb',33),
(3,'cc',35),
(4,'DD',40)

# ===== Cell 15 =====
%sql
select * from salesdb.exttable

# ===== Cell 16 =====
%sql
OPTIMIZE salesDB.exttable

# ===== Cell 17 =====
%sql 
select * from salesDB.mantable;

# ===== Cell 18 =====
%md
**TIME TRAVEL**

# ===== Cell 19 =====
%md
## Managed VS External Delta Tables

# ===== Cell 20 =====
%md
**External Table**

# ===== Cell 21 =====
%sql
CREATE TABLE salesDB.mantable  
(
  id INT,
  name STRING,
  marks INT
)
USING DELTA

# ===== Cell 22 =====
%sql
VACUUM salesdb.exttable;

# ===== Cell 23 =====
%md
### AUTO LOADER

# ===== Cell 24 =====
%md
**DELETE**

# ===== Cell 25 =====
%sql
DESCRIBE HISTORY salesdb.exttable;

# ===== Cell 26 =====
%sql
VACUUM salesdb.exttable RETAIN 0 HOURS;

# ===== Cell 27 =====
%sql 
select * from salesDB.exttable;

# ===== Cell 28 =====
%sql
INSERT INTO salesDB.exttable 
VALUES
(5,'aa',30),
(6,'bb',33),
(7,'cc',35),
(8,'DD',40)

# ===== Cell 29 =====
%sql
OPTIMIZE salesdb.exttable ZORDER BY (id)

# ===== Cell 30 =====
%md
**VACUUM**

# ===== Cell 31 =====
%md
## Delta Tables Functionalities

# ===== Cell 32 =====
%sql
DELETE FROM salesdb.exttable 
WHERE id = 8

# ===== Cell 33 =====
%md
**DATA VERSIONING**

# ===== Cell 34 =====
%sql
select * from salesdb.exttable

# ===== Cell 35 =====
%sql
RESTORE TABLE salesdb.exttable TO VERSION AS OF 2;

# ===== Cell 60 =====
%sql
DROP TABLE salesDB.mantable;

# ===== Cell 61 =====
%sql
CREATE TABLE salesDB.exttable  
(
  id INT,
  name STRING,
  marks INT 
)
USING DELTA    
LOCATION 'abfss://destination@datalakeansh.dfs.core.windows.net/salesDB/exttable'

# ===== Cell 62 =====
df.writeStream.format('delta')\
               .option('checkpointLocation','abfss://aldestination@datalakeansh.dfs.core.windows.net/checkpoint')\
               .option('mergeSchema','true')\
               .trigger(processingTime='5 seconds')\
               .start('abfss://aldestination@datalakeansh.dfs.core.windows.net/data')

# ===== Cell 63 =====
%md
**Streaming Dataframe**

# ===== Cell 64 =====
%sql
CREATE DATABASE salesDB;

# ===== Cell 65 =====
%sql
select * from salesDB.exttable

# ===== Cell 66 =====
%md
### DELTA Table Optimization

# ===== Cell 67 =====
%md 
**ZORDER BY**

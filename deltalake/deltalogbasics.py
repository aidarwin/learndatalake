# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Delta Lake
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo-whitebackground.png" width=200/>
# MAGIC 
# MAGIC by Jacek Laskowski (jacek@japila.pl)

# COMMAND ----------

#Modified by Peter O'Gorman peter@aidarwin.com.au 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Welcome
# MAGIC 
# MAGIC This is one of the modules of the Spark and Delta Lake workshop to teach you how to use and think like a Spark SQL and Delta Lake pro.
# MAGIC 
# MAGIC This Databricks notebook teaches you [Delta Lake](https://delta.io/) with SQL (as the default language). Enjoy!

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Learning Resources
# MAGIC 
# MAGIC The recommended learning resources (for reading and watching) to get better equipped for the module:
# MAGIC 
# MAGIC 1. [Apache Spark](https://spark.apache.org)
# MAGIC 1. [Delta Lake](https://delta.io)
# MAGIC 1. [The Internals of Delta Lake](https://books.japila.pl/delta-lake-internals/)
# MAGIC 1. [Ensuring Consistency with ACID Transactions with Delta Lake (Loan Risk Data)](https://pages.databricks.com/rs/094-YMS-629/images/01-Delta%20Lake%20Workshop%20-%20Delta%20Lake%20Primer.html)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Delta Lake
# MAGIC 
# MAGIC [Slides](https://docs.google.com/presentation/d/1bkxEGDKYZoMbk7Cit8yZ5QZu6dgJybLyAxoot_VCcCY/edit?usp=sharing)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Introduction
# MAGIC 
# MAGIC 1. [Overview](https://books.japila.pl/delta-lake-internals/overview/)
# MAGIC 1. [Installation](https://books.japila.pl/delta-lake-internals/installation/)
# MAGIC 1. Optimization layer on top of a blob storage for reliability (ACID compliance) and low latency of streaming and batch data pipelines
# MAGIC     * Eventually consistent
# MAGIC     * Pretends to be a file system
# MAGIC 1. In short, the `delta` format is `parquet` with a transaction log (`_delta_log` directory) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Default File Format (Databricks)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC spark.sessionState.conf.defaultDataSourceName

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.sql.sources.default

# COMMAND ----------

spark.conf.get('spark.sql.sources.default')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Creating Delta Table

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### CONVERT TO DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 1. Convert existing parquet tables

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS demo_table;
# MAGIC DROP TABLE IF EXISTS cricket_runs_table;
# MAGIC DROP TABLE IF EXISTS cricket_scores_table;
# MAGIC DROP TABLE IF EXISTS demo_table;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.rand.html

# COMMAND ----------

# MAGIC %sql
# MAGIC --check if our cricket demo tables exist
# MAGIC SHOW TABLES LIKE 'cricket_runs_table';
# MAGIC SHOW TABLES LIKE 'cricket_scores_table'

# COMMAND ----------

#this will error if no tables
#otherwise describes the table structure
%sql

DESC EXTENDED cricket_runs_table;
DESC EXTENDED cricket_scores_table;


# COMMAND ----------

#this will error if no tables
#otherwise lists files in folder

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/user/hive/warehouse/cricket_runs_table/

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE cricket_runs_table
# MAGIC -- USING delta
# MAGIC AS VALUES (0, 'Gilcrest', 0), (1, 'Gilcrest', 4), (2, 'Gilcrest', 1) t(ball, batter, runs)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE cricket_scores_table
# MAGIC -- USING delta
# MAGIC AS VALUES (0, 'Gilcrest', 0), (1, 'Gilcrest', 4), (2, 'Gilcrest', 5) t(ball, batter, score)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM cricket_runs_table

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC SELECT * FROM cricket_scores_table

# COMMAND ----------

# Let's check what the file structure looks like

#dbfs:/user/hive/warehouse/cricket_runs_table/_delta_log/ _delta_log/ 0
#dbfs:/user/hive/warehouse/cricket_runs_table/part-00000-2e01d8d9-b129-44cf-a416-565b6f265568-c000.snappy.parquet part-00000-2e01d8d9-b129-44cf-a416-565b6f265568-c000.snappy.parquet 987
# dbfs:/user/hive/warehouse/cricket_runs_table/part-00001-6e7a8bf2-0e35-499a-8af7-7776853d401e-c000.snappy.parquet part-00001-6e7a8bf2-0e35-499a-8af7-7776853d401e-c000.snappy.parquet 987
# dbfs:/user/hive/warehouse/cricket_runs_table/part-00002-df1ca4bf-334b-4a7e-b553-f277eca780d9-c000.snappy.parquet part-00002-df1ca4bf-334b-4a7e-b553-f277eca780d9-c000.snappy.parquet 987


# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/user/hive/warehouse/cricket_runs_table/

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/user/hive/warehouse/cricket_runs_table/_delta_log

# COMMAND ----------

dbutils.fs.head("dbfs:/user/hive/warehouse/cricket_runs_table/_delta_log/00000000000000000000.json")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's use [CONVERT TO DELTA](https://books.japila.pl/delta-lake-internals/sql/#convert-to-delta) SQL command.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- THis is just to show how to convert a table to delta however remember the default file format is delta so no difference
# MAGIC CONVERT TO DELTA cricket_runs_table;

# COMMAND ----------

#lets create some more data

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO cricket_runs_table VALUES
# MAGIC   (4, 'Bevan',0),
# MAGIC   (5, 'Bevan',1)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- lets check the SQL table
# MAGIC SELECT * FROM cricket_runs_table

# COMMAND ----------

#whats wrong ?  We inserted the wrong ball number ie we skipped 3

# COMMAND ----------

#lets check the delta folder

# COMMAND ----------

# lets check the files again

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/user/hive/warehouse/cricket_runs_table/

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/user/hive/warehouse/cricket_runs_table/_delta_log

# COMMAND ----------

#notice there is another file in the log file 
#lets look at it
#the file contains the changes to the data which was inserting ball 4 and 5 for batter Michael Bevan

# COMMAND ----------

dbutils.fs.head("dbfs:/user/hive/warehouse/cricket_runs_table/_delta_log/00000000000000000001.json")

# COMMAND ----------

#lets update the data

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE cricket_runs_table
# MAGIC SET ball = 3 WHERE ball = 4;
# MAGIC 
# MAGIC UPDATE cricket_runs_table
# MAGIC SET ball = 4 where ball = 5;

# COMMAND ----------

#we just updated two rows so lets check the delta log files
# notice there are two more files because we did two updates

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/user/hive/warehouse/cricket_runs_table/_delta_log

# COMMAND ----------

dbutils.fs.head("dbfs:/user/hive/warehouse/cricket_runs_table/_delta_log/00000000000000000002.json")

# COMMAND ----------

dbutils.fs.head("dbfs:/user/hive/warehouse/cricket_runs_table/_delta_log/00000000000000000003.json")

# COMMAND ----------

#note we can achieve the same command with the %fs head command

# COMMAND ----------

# MAGIC %fs head /user/hive/warehouse/cricket_runs_table/_delta_log/00000000000000000003.json

# COMMAND ----------

#lets add some more data to demonstrate the next part of the exercise - checkpoints

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cricket_runs_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cricket_scores_table

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO cricket_runs_table VALUES
# MAGIC   (5, 'Gilcrest',2);
# MAGIC 
# MAGIC   
# MAGIC   INSERT INTO cricket_scores_table VALUES
# MAGIC   (3, 'Bevan',0),
# MAGIC   (4, 'Bevan',1),
# MAGIC   (5, 'Gilcrest',7)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cricket_runs_table ORDER BY ball

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cricket_scores_table ORDER BY ball

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Delta Checkpoints

# COMMAND ----------

#we are going to add 5 more records to the runs so that delta has 10 json files in the delta log

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/user/hive/warehouse/cricket_runs_table/_delta_log

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO cricket_runs_table VALUES
# MAGIC   (6, 'Bevan',0);
# MAGIC INSERT INTO cricket_runs_table VALUES
# MAGIC   (7, 'Bevan',0);
# MAGIC INSERT INTO cricket_runs_table VALUES
# MAGIC   (8, 'Bevan',0);
# MAGIC INSERT INTO cricket_runs_table VALUES
# MAGIC   (9, 'Bevan',0);
# MAGIC INSERT INTO cricket_runs_table VALUES
# MAGIC   (10, 'Bevan',0);
# MAGIC INSERT INTO cricket_runs_table VALUES
# MAGIC   (11, 'Bevan',1);
# MAGIC   
# MAGIC 
# MAGIC INSERT INTO cricket_scores_table VALUES
# MAGIC   (6, 'Bevan',1);
# MAGIC INSERT INTO cricket_scores_table VALUES
# MAGIC   (7, 'Bevan',1);
# MAGIC INSERT INTO cricket_scores_table VALUES
# MAGIC   (8, 'Bevan',1);
# MAGIC INSERT INTO cricket_scores_table VALUES
# MAGIC   (9, 'Bevan',1);
# MAGIC INSERT INTO cricket_scores_table VALUES
# MAGIC   (10, 'Bevan',1);
# MAGIC INSERT INTO cricket_scores_table VALUES
# MAGIC   (11, 'Bevan',2);
# MAGIC   

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/user/hive/warehouse/cricket_runs_table

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/user/hive/warehouse/cricket_runs_table/_delta_log

# COMMAND ----------

# MAGIC %fs head /user/hive/warehouse/cricket_runs_table/_delta_log/00000000000000000010.checkpoint.parquet

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Delta Lake Time Travel

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 1. https://docs.delta.io/latest/quick-start.html

# COMMAND ----------

from delta.tables import *
DeltaTable.convertToDelta(identifier='demo_table', sparkSession=spark)

# COMMAND ----------

df= spark.read.format("delta").option("versionAsOf", 0).load("dbfs:/user/hive/warehouse/cricket_runs_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM cricket_runs_table TIMESTAMP AS OF "2022-04-05 09:20:41.000"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM cricket_runs_table TIMESTAMP AS OF "2022-04-05 09:27:41.000"

# COMMAND ----------

df = spark.read \
  .format("delta") \
  .option("timestampAsOf", "2022-04-05 09:20:41") \
  .load("/user/hive/warehouse/cricket_runs_table")

# COMMAND ----------

df.show()

# COMMAND ----------

df = spark.read \
  .format("delta") \
  .option("timestampAsOf", "2022-04-05 09:27:41") \
  .load("/user/hive/warehouse/cricket_runs_table")

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Setting UTC

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "UTC+10")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Transaction Log

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Use [DESCRIBE DETAIL](https://books.japila.pl/delta-lake-internals/sql/#describe-detail) that comes with Delta Lake

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE DETAIL delta_demo

# COMMAND ----------

spark.table("delta_demo").display()

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/delta_demo/_delta_log/

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Last Section

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's check some Spark Commands

# COMMAND ----------

spark.table('cricket_runs_table').sort('ball').display()

# COMMAND ----------

spark.table('cricket_runs_table').display()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

# display(spark.read.format("parquet").load("dbfs:/user/hive/warehouse/delta_demo/"))

# COMMAND ----------

# spark.range(5).write.format("parquet").save("dbfs:/user/hive/warehouse/delta_demo/")

# COMMAND ----------

# spark.read.load("dbfs:/user/hive/warehouse/delta_demo/").display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### DELETE

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Running `DELETE` on the Delta Lake table
# MAGIC DELETE FROM delta_demo WHERE id = 1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### UPDATE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Attempting to run `UPDATE` on the Delta Lake table
# MAGIC UPDATE delta_demo SET name = 'deux' WHERE id = '2';
# MAGIC 
# MAGIC SELECT * FROM delta_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### MERGE INTO

# COMMAND ----------

# MAGIC %md [Demo: Merge Operation](https://books.japila.pl/delta-lake-internals/demo/merge-operation/)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Time Travel

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC As you modify a Delta table, every operation is automatically versioned.
# MAGIC 
# MAGIC You can query by:
# MAGIC 
# MAGIC 1. Using a timestamp
# MAGIC 1. Using a version number
# MAGIC 
# MAGIC 
# MAGIC using Python, Scala, and/or Scala syntax; for these examples we will use the SQL syntax.
# MAGIC 
# MAGIC For more information, refer to [Introducing Delta Time Travel for Large Scale Data Lakes](https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### DESCRIBE HISTORY

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta_demo

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### VERSION AS OF

# COMMAND ----------

spark.table('delta_demo').display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta_demo VERSION AS OF 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta_demo VERSION AS OF 1

# COMMAND ----------

spark.read.option('versionAsOf', 1).table('delta_demo').display()

# COMMAND ----------



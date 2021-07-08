# Summary

This repo creates a Postgres database (SparkifyDB) with a star schema and an ETL pipeline that extracts data from local song and activity logs directories, process it and loads it to the SparkifyDB.

The purpose of this repo is to support the analytics team conduct analysis on users' song plays.

The selected schema optimises for song play analysis using a single Fact table (songplays) and four Dimensional tables (users, songs, artists, time) which contain data on user identification, database songs and artists as well as song time plays.

# Repo files

sql_queries.py: py file containing the queries  in string format
create_tables.py: py script that creates and drops tables based on sql_queries
etl.py: py script that extracts, process and loads local song and activity data to the database

test.ipynb: Jupyter notebook that loads sql makes a connection to the database and makes queries to its tables as part of testing
etl.ipynb: Jupyter notebook that better explains the input of etl.py
analysis.ipyb: Jupyter notebook to optionally use for analysis, it connects to the SparkifyDB after running create_tables.py and etl.py

# How to use
You can use the command line to run create_tables.py and then etl.py. Then you could use a Jupyter notebook to run SQL queries on the SparkifyDB tables.

Alternatively, you can use the analysis.ipynb file which runs the two scripts and makes a connection to the database. Then you can run any SQL query as necessary.
Some query examples are provided in the analysis.ipynb file.
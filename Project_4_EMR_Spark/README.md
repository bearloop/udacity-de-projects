## Project Summary
This project connects to a Spark session and reads song and logs data from an S3 bucket ('udacity-dent'). After reading the data it constructs 5 tables and loads them to a different bucket ('udacity-sparkify-bucket').

## Repo Files
dl.cfg and etl.py
You need to update the dl.cfg with your AWS credentials before running etl.py.
The etl.py file is the script that connects to Spark, reads the song/log data, transforms them into tables and writes them into an S3 bucket.

## How to use
Create and configure an EMR cluster, connect to the cluster via SSH and execute the etl.py file by spark-submit
# Data Lake with Spark

This repo contains my solution for the Data Lake project of the [Udacity Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027).

## Purpose

The project involves an imaginary music streaming startup called Sparkify. Sparkify would like to setup a data lake to enable their analytics team to analyze the streaming habits of the users. Sparkify has song metadata as well as user activity logs, both as JSON files stored in S3. Data is a subset of the [Million Song Dataset](http://millionsongdataset.com/) [1].

For the data lake we need to setup an ETL pipeline using Apache Spark that reads data from S3, converts the data into dimensional tables, and saves the result back into S3.

## Database schema design

To enable analytics queries, I will transform the data into a star schema with one fact table `songplays` and four dimension tables `songs`, `artists`, `users`, and `time`. This will allow aggregating songplays while filtering, grouping and sorting using the dimensions.

## ETL pipeline

The ETL pipeline processes the two sets of data in two separate steps:

1. From the song data, the `songs` and `artists` tables are created. Duplicate artists are removed, and the results are saved into parquet files. The `songs` table is partitioned by year and artist which should ensure a good distribution of data, assuming many artists publish songs every year.
2. The log data is filtered to only include logs of type `NextSong`. From the filtered data `users` are extracted into a separate table (dropping duplicates). Timestamps are parsed and converted into time components stored in the `time` table. The `songplays` table is constructed using the log data and joining into the song data to resolve song ID and artist ID. A left join is used to preserve log data even with missing song information. The three tables are saved as parquet files. The two tables `time` and `songplays` are expected to be large and should be partitioned properly. The tables are partitioned using year and month which should give a good distribution of data assuming song plays are well distributed over time.

## How to run

The code requires a Python 3 environment to run, with the `pyspark` package installed. Also Java JDK8 and Apache Spark must be installed.

Additionally, an AWS S3 bucket be available, and AWS credentials must be configured in `dl.cfg`. Please create this file based on `dl.cfg.example`.

Run the following script to run the ETL pipeline:

```
python3 etl.py
```

## References

[1] Thierry Bertin-Mahieux, Daniel P.W. Ellis, Brian Whitman, and Paul Lamere. The Million Song Dataset. In Proceedings of the 12th International Society for Music Information Retrieval Conference (ISMIR 2011), 2011.
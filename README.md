# Data Lake for a Music Streaming App 

(This project is the fourth episode in a data engineering project series for a music streaming app, for a design of relational/NoSQL databases of the app, please refer to [Part 1](https://github.com/nd-minh/music-app-data-modeling) and [Part 2](https://github.com/nd-minh/music-app-data-modeling-part-2). For a design of a Data Warehouse for this music app, please refer to [Part 3](https://github.com/nd-minh/music-app-data-warehouse))

In this project, our aim is to design a data lake for a music streaming app. We are given two sources of data as follows:

- Source 1: Logs of user activity on the app, available in JSON format.
Example:

![alt text](/images/log-data.png "log-ex")

- Source 2: Metadata of available songs in the app, available also in JSON format.

Example:

```JSON
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

Both the sources of data are stored in Amazon S3. Our task for this project is to build an ETL pipeline that performs the following steps: 
1. Extract the data from two sources in S3 
2. Pull the relevant information to put into dimension tables using Spark 
3. Load the data back into S3 as a set of dimensional tables 

The dimension tables are designed so that they are optimized for queries on song play analysis. To that end, we propose a star schema and an ETL pipeline to transfer the data from the two sources in S3 into one fact tables and four dimension tables. The description of the tables and their relationship is as follows.

#### Dimension Tables:
- Table `Artists`: contains information about artists in the app. *Columns:* artist_id, name, location, latitude, longitude. 
- Table `Songs`: contains information about songs in the app. *Columns:* song_id, title, artist_id, year, duration. 
- Table `Users`: contains information about users in the app. *Columns:* user_id, first_name, last_name, gender, level. 
- Table `Time`: contains information about timestamps of records broken down into specific units. *Columns:* start_time, hour, day, week, month, year, weekday.

#### Fact Table:
- Tables `SongPlays`: contains records in log data associated with page `NextSong`. *Columns:* songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent. 

With this design, the analytics team can easily query songs that users are listening to, as well as related information about the users, artists, and temporal information of the listening sessions. The details of the extracting, staging, and transforming processes are documented in `etl.py`. Instruction to run the ETL pipeline is given belows.

### Build Instruction
1. Run `python etl.py` to run the ETL pipeline (extract data from S3, pull the relevant information using Spark, load the data back into S3 as a set of dimensional tables).

**Appendix:** Configuration information to connect to Amazon S3 is stored in `dwh.cfg`.  


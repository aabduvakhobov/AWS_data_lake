# Data Lake on S3 with EMR
This project aims to build **Analytical Table** for reporting and analytical purposes from **Data Lake on S3**.

### Motivation
The following analytical tables for data warehouse  are built for imaginary music streaming application called "Sparkify". The startup aims to establish consistent reporting service to analyze the activity of their users. Their data resides in S3 bucket in a directory of JSON logs as well as the directory of JSON metadata on their songs in the app.
### Objective
The main objective of the project is to build ETL pipeline to extract data from S3 and process them with Spark on Amazon EMR clusters and save them back to S3 bucket. Afterwards, the data in S3 must be transformed into a set of dimensional tables that work best for read-intensive purposes.
### Datasets 
There two basic log files in S3 bucket:<br>
- song_data (song metadata)
- log_data (event log)

Data Warehouse design follows a star schema optimized for queries on song play analysis. It consists of the following entities
1. **songplays** - fact table, records in event data associated with song plays:
   _songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent_
2. **users** - users in the app: _user_id, first_name, last_name, gender, level_
3. **songs** - songs in the music database: _song_id, title, artist_id, year, duration_
4. **artists** - artists in the music database: _artist_id, name, location, lattitude, longitude_
5. **time** - timestamps of records in songplays broken down into specific units: _start_time, hour, day, week, month, year, weekday_


### Project files
`etl.py` - to load data from S3 to the memory EMR cluster worker nodes. Then the data is further processed into analytical tables via Spark
`dl.cfg` - config file to hold AWS Iam user credentials.
`etl.ipynb` - jupyter notebook for testing purposes
`README.md` - readme file with instructions

### Sample queries
Run the following queries to check the information in tables:
`SELECT * FROM column_name;`
Run the following query to check table_name, column_name and data_type:
`SELECT 
    table_name, column_name, data_type 
FROM information_schema.columns
WHERE table_name = 'table_name'`


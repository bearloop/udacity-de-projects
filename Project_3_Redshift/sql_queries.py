import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get('S3','LOG_DATA')
SONG_DATA = config.get('S3','SONG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
ARN = config.get('IAM_ROLE','ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


####################################################
# # CREATE TABLES
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
            artist varchar,
            auth varchar,
            firstName varchar,
            gender varchar,
            itemInSession varchar,
            lastName varchar,
            length numeric,
            level varchar,
            location varchar,
            method varchar,
            page varchar,
            registration varchar,
            sessionId int,
            song varchar,
            status int,
            ts bigint,
            userAgent varchar,
            userId int
    )
""")
                
staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
            num_songs int,
            artist_id varchar,
            artist_latitude numeric,
            artist_location varchar,
            artist_longitude numeric,
            artist_name varchar,
            duration numeric,
            song_id varchar,
            title varchar,
            year int
    ) 
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
            songplay_id bigint identity(0, 1) PRIMARY KEY,
            start_time timestamp,
            user_id int,
            level varchar,
            song_id varchar,
            artist_id varchar,
            session_id int,
            location varchar,
            user_agent varchar
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
            user_id int PRIMARY KEY,
            first_name varchar,
            last_name varchar,
            gender varchar,
            level varchar
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar PRIMARY KEY,
        title varchar,
        artist_id varchar,
        year int,
        duration numeric
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
            artist_id varchar PRIMARY KEY,
            name varchar,
            location varchar,
            latitude numeric,
            longitude numeric
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
            start_time timestamp PRIMARY KEY,
            hour int NOT NULL,
            day int NOT NULL,
            week int NOT NULL,
            month int NOT NULL,
            weekday int NOT NULL,
            year int NOT NULL
    )
""")

# # STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    FORMAT AS JSON {};
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    FORMAT AS JSON 'auto';
""").format(SONG_DATA, ARN)

# # FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    SELECT e.start_time, e.userId, e.level, s.song_id, s.artist_id, e.sessionId, e.location, e.userAgent
    FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, * FROM staging_events WHERE page='NextSong') e   
    LEFT JOIN staging_songs s
    ON e.song = s.title AND e.artist = s.artist_name AND e.length = s.duration;
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT e.userId AS user_id,
                e.firstName AS first_name,
                e.lastName AS last_name,
                e.gender,
                e.level
    FROM staging_events e
    WHERE page='NextSong' AND e.userId IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT s.song_id,
           s.title,
            s.artist_id,
           s.year,
           s.duration
    FROM staging_songs s;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT s.artist_id,
           s.artist_name AS name,
           s.artist_location AS location,
           s.artist_latitude AS latitude,
           s.artist_longitude AS longitude
    FROM staging_songs s;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, weekday, year)
    SELECT DISTINCT start_time,
                    EXTRACT(HOUR FROM start_time) AS hour,
                    EXTRACT(DAY FROM start_time) AS day,
                    EXTRACT(WEEK FROM start_time) AS week,
                    EXTRACT(MONTH FROM start_time) AS month,
                    EXTRACT(DOW FROM start_time) AS weekday,
                    EXTRACT(YEAR FROM start_time) AS year
    FROM (SELECT DISTINCT ts,'1970-01-01'::date + ts/1000 * interval '1 second' as start_time FROM staging_events);    
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
import configparser

# Loading the CONFIG variables
config = configparser.ConfigParser()
config.read('dwh.cfg')

S3_LOG_DATA = config.get('S3','LOG_DATA')
S3_LOG_JSON_PATH = config.get('S3', 'LOG_JSON_PATH')
S3_SONG_DATA = config.get('S3', 'SONG_DATA')
DWH_IAM_ROLE_ARN = config.get('IAM_ROLE', 'ARN')
REGION = config.get('GEO', 'REGION')

# Queries for dropping tables
staging_events_table_drop = " DROP TABLE IF EXISTS staging_events ; "
staging_songs_table_drop = " DROP TABLE IF EXISTS staging_songs ; "

songplay_table_drop = " DROP TABLE IF EXISTS songplay_table ; "
user_table_drop = " DROP TABLE IF EXISTS user_table ; "
song_table_drop = " DROP TABLE IF EXISTS song_table ; "
artist_table_drop = " DROP TABLE IF EXISTS artist_table ; "
time_table_drop = " DROP TABLE IF EXISTS time_table ; "

# Queries for creating tables
staging_events_table_create= ("""
    CREATE TABLE staging_events(
        artist  VARCHAR,
        auth    VARCHAR, 
        firstName  VARCHAR,
        gender  VARCHAR,   
        itemInSession  INTEGER,
        lastName VARCHAR,
        length  FLOAT,
        level VARCHAR, 
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration FLOAT,
        sessionId  INTEGER,
        song  VARCHAR,
        status INTEGER,
        ts  TIMESTAMP,
        userAgent VARCHAR,
        userId INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
      num_songs INTEGER,
      artist_id VARCHAR,
      artist_latitude FLOAT,
      artist_longitude FLOAT,
      artist_location VARCHAR,
      artist_name VARCHAR,
      song_id VARCHAR,
      title VARCHAR,
      duration FLOAT,
      year INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay_table (
        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL SORTKEY,
        user_id INTEGER NOT NULL REFERENCES user_table(user_id),
        level VARCHAR,
        song_id VARCHAR NOT NULL REFERENCES song_table(song_id),
        artist_id VARCHAR NOT NULL REFERENCES artist_table(artist_id),
        session_id INTEGER NOT NULL,
        location VARCHAR,
        user_agent VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS user_table (
        user_id INTEGER SORTKEY PRIMARY KEY,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        gender CHAR(1),
        level VARCHAR NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song_table
    (
        song_id VARCHAR SORTKEY PRIMARY KEY,
        title TEXT NOT NULL,
        artist_id VARCHAR NOT NULL DISTKEY REFERENCES artist_table(artist_id),
        year INTEGER NOT NULL,
        duration DECIMAL NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist_table
    (
        artist_id VARCHAR PRIMARY KEY SORTKEY,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude DECIMAL,
        longitude DECIMAL 
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time_table
    (
        start_time TIMESTAMP NOT NULL PRIMARY KEY SORTKEY,
        hour NUMERIC NOT NULL,
        day NUMERIC NOT NULL,
        week NUMERIC NOT NULL,
        month NUMERIC NOT NULL,
        year NUMERIC NOT NULL,
        weekday NUMERIC NOT NULL
    );
""")

# Queries for loading data into the stagging tables from the files present in S3
staging_events_copy = ("""
    COPY staging_events 
    FROM {}
    iam_role '{}'
    region {}
    FORMAT AS json {}
    timeformat 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(S3_LOG_DATA, DWH_IAM_ROLE_ARN, REGION, S3_LOG_JSON_PATH)

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {}
    iam_role '{}'
    region {}
    FORMAT AS json 'auto'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(S3_SONG_DATA, DWH_IAM_ROLE_ARN, REGION)

# Queries for transforming and loading data from the stagging tables into the fact & dimentional tables 
songplay_table_insert = ("""
INSERT INTO songplay_table (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS') AS start_time,
        se.userId AS user_id,
        se.level AS level,
        ss.song_id AS song_id,
        ss.artist_id AS artist_id,
        se.sessionId AS session_id,
        se.location AS location,
        se.userAgent AS user_agent
    FROM staging_events se
    JOIN staging_songs ss ON (se.song = ss.title AND se.artist = ss.artist_name)
    AND se.page  =  'NextSong'
""")

user_table_insert = ("""
INSERT INTO user_table (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT(userId) AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender,
        level
    FROM staging_events
    WHERE page = 'NextSong' 
    AND user_id NOT IN (SELECT DISTINCT user_id FROM user_table);
""")

song_table_insert = ("""
INSERT INTO song_table (song_id, title, artist_id, year, duration)
    SELECT DISTINCT(song_id) AS song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id NOT IN (SELECT DISTINCT song_id FROM song_table);
""")

artist_table_insert = ("""
INSERT INTO artist_table (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT(artist_id) as artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
    FROM staging_songs
    WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artist_table);
""")

time_table_insert = ("""
INSERT INTO time_table (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT(start_time) AS start_time,
            EXTRACT(hour FROM start_time) AS hour,
            EXTRACT(day FROM start_time) AS day,
            EXTRACT(week FROM start_time) AS week,
            EXTRACT(month FROM start_time) AS month,
            EXTRACT(year FROM start_time) AS year,
            EXTRACT(dayofweek FROM start_time) as weekday
    FROM songplay_table;
""")

#Analytical Query
count_staging_events = """ SELECT COUNT(*) FROM staging_events ;"""
count_staging_songs = """ SELECT COUNT(*) FROM staging_songs ;"""
count_songplay_table = """ SELECT COUNT(*) FROM songplay_table ;"""
count_user_table = """ SELECT COUNT(*) FROM user_table ;"""
count_song_table = """ SELECT COUNT(*) FROM song_table ;"""
count_artist_table = """ SELECT COUNT(*) FROM artist_table ;"""
count_time_table = """ SELECT COUNT(*) FROM time_table ;"""

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, time_table_create, song_table_create, songplay_table_create ]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

create_tables_name = [ 'staging_events' , 'staging_songs', 'user_table', 'artist_table', 'time_table', 'song_table', 'songplay_table' ]
copy_tables_name = ['staging_events','staging_songs']
insert_tables_name = ['songplay_table','user_table','song_table','artist_table','time_table']
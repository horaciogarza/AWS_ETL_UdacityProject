import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

events_stage_table_drop = "DROP TABLE IF EXISTS events_stage"
songs_stage_table_drop = "DROP TABLE IF EXISTS songs_stage"

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
TIME_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

events_stage_table_create= ("""CREATE TABLE IF NOT EXISTS events_stage
                                    (
                                        artist        VARCHAR,
                                        auth          VARCHAR,
                                        firstName     VARCHAR,
                                        gender        VARCHAR,
                                        itemInSession VARCHAR,
                                        lastName      VARCHAR,
                                        length        DOUBLE PRECISION,
                                        level         VARCHAR,
                                        location      VARCHAR,
                                        method        VARCHAR,
                                        page          VARCHAR,
                                        registration  VARCHAR,
                                        sessionId     INTEGER,
                                        song          VARCHAR,
                                        status        INTEGER,  
                                        ts            BIGINT,
                                        userAgent     VARCHAR,
                                        userId        INTEGER,
                                        
                                        
                                    )
""")

songs_stage_table_create = (""" CREATE TABLE IF NOT EXISTS songs_stage
                                    (
                                        num_songs           INTEGER,
                                        artist_id           VARCHAR,
                                        artist_latitude     VARCHAR,
                                        artist_longitude    VARCHAR,
                                        artist_location     VARCHAR,
                                        artist_name         VARCHAR,
                                        song_id             VARCHAR,
                                        title               VARCHAR,
                                        duration            DOUBLE PRECISION,
                                        year                INTEGER,
                                        primary key(artist_id),
                                        foreign key(song_id) references songs(song_id)
                                    )
""")

songplay_table_create = (""" CREATE TABLE songplays 
                                (
                                    songplay_id   INT IDENTITY(0,1),
                                    start_time    TIMESTAMP,
                                    user_id       VARCHAR,
                                    level         VARCHAR,
                                    artist_id     VARCHAR,
                                    song_id       VARCHAR,
                                    session_id    VARCHAR,
                                    location      VARCHAR,
                                    user_agent    VARCHAR,
                                    primary key(songplay_id),
                                    foreign key(user_id) references users(user_id),
                                    foreign key(artist_id) references artists(artist_id),
                                    foreign key(song_id) references songs(song_id)
                                )
""")

user_table_create = (""" CREATE TABLE users
                            (
                                user_id       INTEGER,
                                first_name    VARCHAR,
                                last_name     VARCHAR, 
                                gender        VARCHAR,
                                level         VARCHAR,
                                primary key(user_id)

                            ) diststyle all
""")

song_table_create = ("""    CREATE TABLE songs
                                (
                                    song_id       VARCHAR,
                                    title         VARCHAR,
                                    artist_id     VARCHAR,
                                    year          INTEGER,
                                    duration      DOUBLE PRECISION,
                                    primary key(song_id),
                                    foreign key(artist_id) references listing(listid)
                                )
""")

artist_table_create = ("""  CREATE TABLE artists
                                (
                                    artist_id     VARCHAR,
                                    name          VARCHAR, 
                                    location      VARCHAR, 
                                    latitude      VARCHAR, 
                                    longitude     VARCHAR,
                                    primary key(artist_id)
                                ) diststyle all
""")

TIME_table_create = (""" CREATE TABLE TIME
                            (
                                start_time    TIMESTAMP,
                                hour          SMALLINT,
                                day           SMALLINT,
                                week          SMALLINT,
                                month         SMALLINT,
                                year          SMALLINT,
                                weekday       SMALLINT
                            ) diststyle all
""")

# STAGING TABLES

events_stage_copy = ("""
                        COPY events_stage FROM {} 
                        CREDENTIALS 'aws_iam_role={}'
                        FORMAT AS JSON {}
                        region 'us-west-2'
                    """).format(config.get('S3','LOG_DATA'),
                                config.get('IAM_ROLE', 'ARN'),
                                config.get('S3','LOG_JSONPATH'))

songs_stage_copy = ("""
                        COPY songs_stage FROM {} 
                        CREDENTIALS 'aws_iam_role={}'
                        FORMAT AS JSON 'auto'
                        region 'us-west-2'
                    """).format(config.get('S3','SONG_DATA'), 
                                config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES




songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time,
        user_id,
        level,
        artist_id,
        song_id,
        session_id,
        location,
        user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + es.ts/1000 * INTERVAL '1 second'   
           es.userId,              
           es.level,             
           ss.artist_id,           
           ss.song_id,          
           es.sessionId,           
           es.location,          
           es.userAgent           
    FROM events_stage AS es
    JOIN songs_stage AS ss
        ON  es.artist = ss.artist_name AND 
            es.length = ss.duration AND
            es.song = ss.title 
    WHERE es.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users(
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level)
    SELECT DISTINCT userId,
           firstName,
           lastName,
           gender,
           level
    FROM events_stage
    WHERE   userId IS NOT NULL AND 
            page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs(
        song_id, 
        title, 
        artist_id, 
        year, 
        duration)
    SELECT DISTINCT song_id,
           title,
           artist_id,
           year,
           duration
    FROM songs_stage
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists(
        artist_id,
        name,
        location,
        latitude,
        longitude)
    SELECT DISTINCT artist_id,
           artist_name,
           artist_location,
           artist_latitude,
           artist_longitude
    FROM songs_stage
    WHERE artist_id IS NOT NULL
""")

TIME_table_insert = ("""
    INSERT INTO time(
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday)
    SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 \
               * INTERVAL '1 second',
           EXTRACT(hour FROM start_time),
           EXTRACT(day FROM start_time),
           EXTRACT(week FROM start_time),
           EXTRACT(month FROM start_time),
           EXTRACT(year FROM start_time),
           EXTRACT(week FROM start_time)
    FROM events_stage
    WHERE page = 'NextSong' AND ts IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [events_stage_table_create, songs_stage_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, TIME_table_create]
drop_table_queries = [events_stage_table_drop, songs_stage_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, TIME_table_drop]
copy_table_queries = [events_stage_copy, songs_stage_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, TIME_table_insert]

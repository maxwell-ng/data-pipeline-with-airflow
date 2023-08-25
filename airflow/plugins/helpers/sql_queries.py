class SqlQueries:
    sql_copy = ("""
        COPY {} 
        FROM '{}' 
        IAM_ROLE '{}'
        JSON '{}';
    """)
    staging_events_create = ("""
        CREATE TABLE public.staging_events (
            artist varchar(256),
            auth varchar(256),
            firstname varchar(256),
            gender varchar(256),
            iteminsession int4,
            lastname varchar(256),
            length numeric(18,0),
            "level" varchar(256),
            location varchar(256),
            "method" varchar(256),
            page varchar(256),
            registration numeric(18,0),
            sessionid int4,
            song varchar(256),
            status int4,
            ts int8,
            useragent varchar(256),
            userid int4
    );
                             """)

    staging_songs_create = ("""
        CREATE TABLE public.staging_songs (
            num_songs int4,
            artist_id varchar(256),
            artist_name super,
            artist_latitude numeric(18,0),
            artist_longitude numeric(18,0),
            artist_location super,
            song_id varchar(256),
            title varchar(256),
            duration numeric(18,0),
            "year" int4
        );
    """)

    songplay_table_create = ("""
        CREATE TABLE public.songplays (
            playid varchar(32) NOT NULL,
            start_time timestamp NOT NULL,
            userid int4 NOT NULL,
            "level" varchar(256),
            songid varchar(256),
            artistid varchar(256),
            sessionid int4,
            location varchar(256),
            user_agent varchar(256),
            CONSTRAINT songplays_pkey PRIMARY KEY (playid)
        );
     """)
    
    songplay_table_insert = ("""
        SELECT
            md5(cast(events.ts as varchar)) AS songplay_id,
            events.start_time, 
            events.userid, 
            events.level, 
            songs.song_id, 
            songs.artist_id, 
            events.sessionid, 
            events.location, 
            events.useragent
            FROM (SELECT cast(TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' as timestamp) AS start_time, *
                    FROM staging_events
                    WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
            AND events.artist = songs.artist_name
            AND events.length = songs.duration
            WHERE songplay_id IS NOT NULL AND events.userid IS NOT NULL AND events.start_time IS NOT NULL
    """)
    
    user_table_create = ("""
                         CREATE TABLE public.users (
                            userid int4 NOT NULL,
                            first_name varchar(256),
                            last_name varchar(256),
                            gender varchar(256),
                            "level" varchar(256),
                            CONSTRAINT users_pkey PRIMARY KEY (userid)
                        );
                         """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE userid IS NOT NULL AND page='NextSong'
    """)

    song_table_create = ("""
                         CREATE TABLE public.songs (
                            songid varchar(256) NOT NULL,
                            title varchar(256),
                            artistid varchar(256),
                            "year" int4,
                            duration numeric(18,0),
                            CONSTRAINT songs_pkey PRIMARY KEY (songid)
                        );
                         """)
    
    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)
    
    artist_table_create = ("""
        CREATE TABLE public.artists (
            artistid varchar(256) NOT NULL,
            name varchar(256),
            location varchar(256),
            lattitude numeric(18,0),
            longitude numeric(18,0)
        );
        """)
    
    artist_table_insert = ("""
        SELECT 
            distinct artist_id, 
            substring(cast(artist_name as varchar), 1, 50) as name, 
            substring(cast(artist_location as varchar), 1,50) as location, 
            artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_create = ("""
                         CREATE TABLE public.time (
                            start_time timestamp NOT NULL,
                            "hour" int4,
                            "day" int4,
                            week int4,
                            "month" varchar(256),
                            "year" int4,
                            weekday varchar(256),
                            CONSTRAINT time_pkey PRIMARY KEY (start_time)
                        ) ;
                         """)
    
    time_table_insert = ("""
        SELECT cast(TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' as timestamp) AS start_time,
            EXTRACT(hour FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS hour,
            EXTRACT(day FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS day,
            EXTRACT(week FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS week,
            EXTRACT(month FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS month,
            EXTRACT(year FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS year,
            EXTRACT(dow FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS weekday
        FROM staging_events
    """)

    dq_checks_stm = [
        {"check_sql": "SELECT COUNT(*) FROM songs WHERE songid is null;", "expected_result": 0},
        {"check_sql": "SELECT COUNT(*) FROM users WHERE userid is null;", "expected_result": 0},
        {"check_sql": "SELECT COUNT(*) FROM artists WHERE artistid is null;", "expected_result": 0},
        {"check_sql": """SELECT *
                            FROM time
                            WHERE start_time IS NULL 
                                OR hour IS NULL 
                                OR day IS NULL 
                                OR week IS NULL 
                                OR month IS NULL 
                                OR year IS NULL 
                                OR weekday IS NULL;""",
        "expected_result": 0},
        {"check_sql": """SELECT *
                        FROM songplays
                        WHERE playid IS NULL
                            OR start_time IS NULL
                            OR userid IS NULL
                            OR level IS NULL
                            OR songid IS NULL
                            OR artistid IS NULL
                            OR sessionid IS NULL;""",
        "expected_result": 0 },
    ]
    
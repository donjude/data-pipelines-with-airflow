class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
<<<<<<< HEAD
    """)
=======
    """)
    
    check_nulls_songplays = ("""
        SELECT COUNT(*) FROM songplays WHERE playid IS NULL    
    """)
    
    check_nulls_songs = ("""
        SELECT COUNT(*) FROM songs WHERE songid IS NULL    
    """)
    
    check_nulls_artists = ("""
        SELECT COUNT(*) FROM artists WHERE artistid IS NULL    
    """)
    
    check_nulls_users = ("""
        SELECT COUNT(*) FROM users WHERE userid IS NULL    
    """)
    
    null_checks = [check_nulls_songplays, check_nulls_songs, check_nulls_artists, check_nulls_users]
    exp_results = [0, 0, 0, 0]
    
    
    
>>>>>>> origin/main

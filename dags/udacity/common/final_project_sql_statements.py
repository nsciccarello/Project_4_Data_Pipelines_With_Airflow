class SqlQueries:
    """
    This class contains SQL queries for loading data into Redshift fact and dimension tables
    from the staging tables.
    """

    # SQL query for inserting data into the fact table 'songplays'
    songplay_table_insert = ("""
        SELECT
            md5(events.sessionid || events.start_time) AS songplay_id, -- Generate a unique ID for each songplay
            events.start_time,                                        -- Timestamp of the songplay
            events.userid,                                            -- User ID
            events.level,                                             -- Subscription level (e.g., free, paid)
            songs.song_id,                                            -- Song ID from 'staging_songs'
            songs.artist_id,                                          -- Artist ID from 'staging_songs'
            events.sessionid,                                         -- Session ID
            events.location,                                          -- User's location
            events.useragent                                          -- User's browser and OS info
        FROM (
            SELECT 
                TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, -- Convert timestamp from milliseconds to datetime
                *
            FROM staging_events
            WHERE page='NextSong'                                      -- Filter only 'NextSong' events
        ) events
        LEFT JOIN staging_songs songs                                  -- Join events with songs on title, artist, and duration
        ON events.song = songs.title
            AND events.artist = songs.artist_name
            AND events.length = songs.duration
    """)

    # SQL query for inserting data into the dimension table 'users'
    user_table_insert = ("""
        SELECT DISTINCT
            userid,              -- Unique user ID
            firstname,           -- User's first name
            lastname,            -- User's last name
            gender,              -- User's gender
            level                -- Subscription level (e.g., free, paid)
        FROM staging_events
        WHERE page='NextSong'    -- Filter only 'NextSong' events
        AND userid IS NOT NULL   -- Exclude rows where 'userid' is NULL
    """)

    # SQL query for inserting data into the dimension table 'songs'
    song_table_insert = ("""
        SELECT DISTINCT
            song_id,             -- Unique song ID
            title,               -- Song title
            artist_id,           -- Artist ID
            year,                -- Release year of the song
            duration             -- Duration of the song
        FROM staging_songs
        WHERE song_id IS NOT NULL -- Exclude rows where 'song_id' is NULL
    """)

    # SQL query for inserting data into the dimension table 'artists'
    artist_table_insert = ("""
        SELECT DISTINCT
            artist_id,           -- Unique artist ID
            artist_name,         -- Artist's name
            artist_location,     -- Artist's location
            artist_latitude,     -- Latitude of the artist's location
            artist_longitude     -- Longitude of the artist's location
        FROM staging_songs
        WHERE artist_id IS NOT NULL -- Exclude rows where 'artist_id' is NULL
    """)

    # SQL query for inserting data into the dimension table 'time'
    time_table_insert = ("""
        SELECT
            start_time,                     -- Timestamp of the songplay
            EXTRACT(hour FROM start_time),  -- Hour of the event
            EXTRACT(day FROM start_time),   -- Day of the event
            EXTRACT(week FROM start_time),  -- Week of the event
            EXTRACT(month FROM start_time), -- Month of the event
            EXTRACT(year FROM start_time),  -- Year of the event
            EXTRACT(dayofweek FROM start_time) -- Day of the week (0 = Sunday)
        FROM songplays
        WHERE start_time IS NOT NULL        -- Exclude rows where 'start_time' is NULL
    """)
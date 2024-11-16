-- Drop existing tables to avoid conflicts during recreation
DROP TABLE IF EXISTS staging_events;
DROP TABLE IF EXISTS staging_songs;
DROP TABLE IF EXISTS songplays;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS songs;
DROP TABLE IF EXISTS artists;
DROP TABLE IF EXISTS time;

-- Staging Tables
-- These tables temporarily store raw data from the source (e.g., S3) before transformation.

-- Staging table for event data (log data)
CREATE TABLE IF NOT EXISTS staging_events (
    artist              VARCHAR,        -- Name of the artist
    auth                VARCHAR,        -- Authentication status (e.g., logged in, logged out)
    firstName           VARCHAR,        -- User's first name
    gender              CHAR(1),        -- User's gender
    itemInSession       INT,            -- Position of the item in the session
    lastName            VARCHAR,        -- User's last name
    length              FLOAT,          -- Length of the song played
    level               VARCHAR,        -- Subscription level (e.g., free, paid)
    location            TEXT,           -- User's location
    method              VARCHAR,        -- HTTP method (e.g., GET, POST)
    page                VARCHAR,        -- Page visited (e.g., NextSong)
    registration        FLOAT,          -- Timestamp of user registration
    sessionId           INT,            -- ID of the session
    song                VARCHAR,        -- Name of the song
    status              INT,            -- HTTP status code
    ts                  BIGINT,         -- Timestamp in epoch format
    userAgent           TEXT,           -- User's browser and OS information
    userId              VARCHAR         -- ID of the user
);

-- Staging table for song data
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs           INT,            -- Number of songs by the artist
    artist_id           VARCHAR,        -- Unique identifier for the artist
    artist_latitude     FLOAT,          -- Latitude of the artist's location
    artist_longitude    FLOAT,          -- Longitude of the artist's location
    artist_location     TEXT,           -- Artist's location
    artist_name         VARCHAR,        -- Artist's name
    song_id             VARCHAR,        -- Unique identifier for the song
    title               VARCHAR,        -- Song title
    duration            FLOAT,          -- Duration of the song
    year                INT             -- Release year of the song
);

-- Fact Table
-- This table stores the main business data for analytics, such as song plays.
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id         VARCHAR     PRIMARY KEY,  -- Unique identifier for each songplay
    start_time          TIMESTAMP   NOT NULL,     -- Start time of the songplay
    userid              VARCHAR,                  -- User ID
    level               VARCHAR,                  -- Subscription level
    song_id             VARCHAR,                  -- Song ID
    artist_id           VARCHAR,                  -- Artist ID
    sessionid           INT,                      -- Session ID
    location            TEXT,                     -- User's location
    useragent           TEXT                      -- User's browser and OS information
);

-- Dimension Tables
-- These tables store descriptive attributes related to fact data.

-- Users dimension table
CREATE TABLE IF NOT EXISTS users (
    userid              VARCHAR     PRIMARY KEY,  -- Unique identifier for the user
    first_name          VARCHAR,                  -- User's first name
    last_name           VARCHAR,                  -- User's last name
    gender              CHAR(1),                  -- User's gender
    level               VARCHAR                   -- Subscription level
);

-- Songs dimension table
CREATE TABLE IF NOT EXISTS songs (
    song_id             VARCHAR     PRIMARY KEY,  -- Unique identifier for the song
    title               VARCHAR,                  -- Song title
    artist_id           VARCHAR,                  -- Artist ID
    year                INT,                      -- Release year of the song
    duration            FLOAT                     -- Duration of the song
);

-- Artists dimension table
CREATE TABLE IF NOT EXISTS artists (
    artist_id           VARCHAR     PRIMARY KEY,  -- Unique identifier for the artist
    name                VARCHAR,                  -- Artist's name
    location            TEXT,                     -- Artist's location
    latitude            FLOAT,                    -- Latitude of the artist's location
    longitude           FLOAT                     -- Longitude of the artist's location
);

-- Time dimension table
CREATE TABLE IF NOT EXISTS time (
    start_time          TIMESTAMP   PRIMARY KEY,  -- Start time of the event
    hour                INT,                      -- Hour of the event
    day                 INT,                      -- Day of the event
    week                INT,                      -- Week of the event
    month               INT,                      -- Month of the event
    year                INT,                      -- Year of the event
    weekday             INT                       -- Day of the week (0 = Sunday, 6 = Saturday)
);

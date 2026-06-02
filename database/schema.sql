CREATE TABLE IF NOT EXISTS job_listings (
    id         SERIAL PRIMARY KEY,
    source     VARCHAR,
    title      VARCHAR,
    company    VARCHAR,
    location   VARCHAR,
    deadline   VARCHAR,
    url        TEXT UNIQUE,
    scraped_at TIMESTAMP DEFAULT NOW()
);
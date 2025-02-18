CREATE TABLE submissions (
    id VARCHAR(255) PRIMARY KEY,
    author VARCHAR(255),
    title TEXT,
    created_utc DATETIME,
    selftext TEXT,
    subreddit VARCHAR(255),
    link_flair_text VARCHAR(255),
    link TEXT,
    num_comments INT,
    score INT
);

CREATE TABLE comments (
    id VARCHAR(255) PRIMARY KEY,
    post_id VARCHAR(255),
    parent_id VARCHAR(255),
    author VARCHAR(255),
    created_utc DATETIME,
    body TEXT,
    depth INT
);
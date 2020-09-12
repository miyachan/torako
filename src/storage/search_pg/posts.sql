CREATE TABLE IF NOT EXISTS posts (
    board VARCHAR(8) NOT NULL,
    thread_no BIGINT NOT NULL,
    post_no BIGINT NOT NULL,
    subject TSVECTOR,
    username TSVECTOR,
    tripcode TSVECTOR,
    email TSVECTOR,
    unique_id TEXT,
    since4_pass INT,
    country TEXT,
    filename TSVECTOR,
    image_hash TEXT,
    image_width INT,
    image_height INT,
    ts TIMESTAMP,
    comment TSVECTOR,
    deleted bool,
    ghost bool,
    sticky bool,
    op bool,
    capcode VARCHAR(1),
    PRIMARY KEY (board, post_no)
);

CREATE INDEX IF NOT EXISTS posts_thread ON posts (thread_no);
CREATE INDEX IF NOT EXISTS posts_subject ON posts USING GIN (subject);
CREATE INDEX IF NOT EXISTS posts_username ON posts USING GIN (username);
CREATE INDEX IF NOT EXISTS posts_tripcode ON posts (tripcode);
CREATE INDEX IF NOT EXISTS posts_email ON posts USING GIN (email);
CREATE INDEX IF NOT EXISTS posts_unqiue_id ON posts (unique_id);
CREATE INDEX IF NOT EXISTS posts_since4pass ON posts (since4_pass);
CREATE INDEX IF NOT EXISTS posts_country ON posts (country);
CREATE INDEX IF NOT EXISTS posts_filename ON posts USING GIN (filename);
CREATE INDEX IF NOT EXISTS posts_im_hash ON posts (image_hash);
CREATE INDEX IF NOT EXISTS posts_im_w ON posts (image_width);
CREATE INDEX IF NOT EXISTS posts_im_h ON posts (image_height);
CREATE INDEX IF NOT EXISTS posts_ts ON posts (ts DESC);
CREATE INDEX IF NOT EXISTS posts_com ON posts USING GIN (comment);
CREATE INDEX IF NOT EXISTS posts_deleted ON posts (deleted);
CREATE INDEX IF NOT EXISTS posts_ghost ON posts (ghost);
CREATE INDEX IF NOT EXISTS posts_sticky ON posts (sticky);
CREATE INDEX IF NOT EXISTS posts_op ON posts (op);
CREATE INDEX IF NOT EXISTS posts_capcode ON posts (capcode);
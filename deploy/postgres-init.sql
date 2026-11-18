-- Schema for Task 2: one row holds the counter, and `version` serves the
-- optimistic-locking variant. Safe to apply more than once.
CREATE TABLE IF NOT EXISTS user_counter (
    user_id int PRIMARY KEY,
    counter int NOT NULL,
    version int NOT NULL
);

INSERT INTO user_counter (user_id, counter, version) VALUES (1, 0, 0)
ON CONFLICT (user_id) DO NOTHING;

-- Task Adv 1: one counter per user. counter-server --store postgres-users
-- writes it directly; the Hazelcast maps likes_write_through and
-- likes_write_behind read and write it through the generic MapLoader and
-- MapStore (deploy/hazelcast-node*.yaml).
CREATE TABLE IF NOT EXISTS user_likes (
    user_id bigint PRIMARY KEY,
    likes bigint NOT NULL
);

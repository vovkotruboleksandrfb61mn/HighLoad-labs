-- Schema for Task 2: one row holds the counter, and `version` serves the
-- optimistic-locking variant. Safe to apply more than once.
CREATE TABLE IF NOT EXISTS user_counter (
    user_id int PRIMARY KEY,
    counter int NOT NULL,
    version int NOT NULL
);

INSERT INTO user_counter (user_id, counter, version) VALUES (1, 0, 0)
ON CONFLICT (user_id) DO NOTHING;

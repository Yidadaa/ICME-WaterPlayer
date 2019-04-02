--
-- 由SQLiteStudio v3.2.1 产生的文件 周二 4月 2 11:41:17 2019
--
-- 文本编码：System
--
PRAGMA foreign_keys = off;
BEGIN TRANSACTION;

-- 表：AUTHOR
CREATE TABLE AUTHOR (
    author_id INTEGER PRIMARY KEY
                    UNIQUE ON CONFLICT IGNORE
                    NOT NULL
);


-- 表：ITEM
CREATE TABLE ITEM (
    item_id     INTEGER PRIMARY KEY
                        UNIQUE ON CONFLICT IGNORE
                        NOT NULL,
    item_city   INTEGER DEFAULT (0),
    duration    INTEGER DEFAULT (0),
    music_id    INTEGER DEFAULT (0),
    face        CHAR,
    title       CHAR,
    video       CHAR,
    audio       CHAR,
    sum_play    INTEGER DEFAULT (0),
    like_rate   DOUBLE  DEFAULT (0),
    finish_rate DOUBLE  DEFAULT (0.0) 
);


-- 表：PLAY_HISTORY
CREATE TABLE PLAY_HISTORY (
    uid               REFERENCES USER (uid),
    item_id           REFERENCES ITEM (item_id),
    author_id         REFERENCES AUTHOR (author_id),
    channel   INTEGER DEFAULT (0),
    finish    BOOLEAN NOT NULL,
    [like]    BOOLEAN NOT NULL,
    device    INTEGER DEFAULT (0),
    time      INTEGER NOT NULL
);


-- 表：USER
CREATE TABLE USER (
    uid       INTEGER PRIMARY KEY
                      UNIQUE ON CONFLICT IGNORE
                      NOT NULL,
    user_city INTEGER DEFAULT (0) 
);


COMMIT TRANSACTION;
PRAGMA foreign_keys = on;


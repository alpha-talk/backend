CREATE TABLE IF NOT EXISTS daily_candle (
    code   CHAR(6)  NOT NULL,
    date   CHAR(8)  NOT NULL,
    open   INTEGER  NOT NULL,
    high   INTEGER  NOT NULL,
    low    INTEGER  NOT NULL,
    close  INTEGER  NOT NULL,
    volume BIGINT   NOT NULL,
    value  BIGINT   NOT NULL,
    PRIMARY KEY (code, date)
);

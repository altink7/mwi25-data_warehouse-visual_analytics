DROP DATABASE IF EXISTS citibike_db;
CREATE DATABASE citibike_db;

\c citibike_db;

CREATE USER citibike_user WITH ENCRYPTED PASSWORD 'citibike';
GRANT ALL PRIVILEGES ON DATABASE citibike_db TO citibike_user;

CREATE TABLE UserType
(
    userTypeId  SERIAL PRIMARY KEY,
    userType    VARCHAR(50) NOT NULL,
    description VARCHAR(250)
);

CREATE TABLE Station
(
    id      VARCHAR(50) PRIMARY KEY,
    name    VARCHAR(50) NOT NULL,
    oldName VARCHAR(50) NULL
);

CREATE TABLE Date
(
    id       SERIAL PRIMARY KEY,
    tripId   VARCHAR(50) NOT NULL,
    hour     INT         NOT NULL,
    day      INT         NOT NULL,
    month    INT         NOT NULL,
    year     INT         NOT NULL,
    minute   INT         NOT NULL,
    dateTime TIMESTAMP   NOT NULL
);

CREATE TABLE FACT_Trip
(
    tripId       VARCHAR(50) PRIMARY KEY,
    startStation VARCHAR(50) NOT NULL,
    stopStation  VARCHAR(50) NOT NULL,
    stopTime     TIMESTAMP   NOT NULL,
    userTypeId   INT         NOT NULL,
    startTime    TIMESTAMP   NOT NULL,
    duration     INTERVAL,

    CONSTRAINT FK_FACT_Trip_startStation FOREIGN KEY (startStation) REFERENCES Station (id),
    CONSTRAINT FK_FACT_Trip_stopStation FOREIGN KEY (stopStation) REFERENCES Station (id),
    CONSTRAINT FK_FACT_Trip_userTypeId FOREIGN KEY (userTypeId) REFERENCES UserType (userTypeId)
);

ALTER TABLE Date
    ADD CONSTRAINT FK_Date_FACT_Trip FOREIGN KEY (tripId) REFERENCES FACT_Trip (tripId);

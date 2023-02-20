CREATE KEYSPACE IF NOT EXISTS bigdata
  WITH REPLICATION = {
   'class' : 'SimpleStrategy',
   'replication_factor' : 1
  };

DROP TABLE IF EXISTS bigdata.tripduration;

CREATE TABLE bigdata.tripduration (
  date TIMESTAMP PRIMARY KEY,
  max float,
  min float,          
  avg float,
  stddev double,
);
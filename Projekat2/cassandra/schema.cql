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
  taxi1 int, 
  count1 int, 
  taxi2 int,
  count2 int,
  taxi3 int, 
  count3 int
);


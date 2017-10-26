CREATE KEYSPACE IF NOT EXISTS dapper WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' };
use dapper;

drop table if exists test;

create TABLE test (id int, value text, primary key (id));
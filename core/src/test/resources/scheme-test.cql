CREATE KEYSPACE IF NOT EXISTS dapper WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' };
use dapper;

drop table if exists test;
drop type if exists location_type;

create type location_type (latitude float, longitude float, accuracy int);

create TABLE test (id int, created timeuuid, value text, params map<ascii, ascii>, names list<text>, ints set<int>, location frozen<location_type>, valueOpt text, tuple tuple<int, text>, primary key (id, created));
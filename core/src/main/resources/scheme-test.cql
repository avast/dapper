CREATE KEYSPACE IF NOT EXISTS dapper WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' };
use dapper;

drop table if exists test;

create TABLE test (id int, created timeuuid, value text, params map<ascii, ascii>, names list<text>, ints set<int>, primary key (id, created));
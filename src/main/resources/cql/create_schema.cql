create keyspace datastax_tickdata_demo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3' };

use datastax_tickdata_demo; 

CREATE TABLE tick_data ( 
	symbol text,
	date timestamp,
	value double,
	PRIMARY KEY (symbol, date)
) WITH CLUSTERING ORDER BY (date DESC);


truncate tick_data;

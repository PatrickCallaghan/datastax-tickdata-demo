Tick Data example
========================================================

This is a simple example of using C* as a tick data store for financial market data.

## Running the demo 

You will need a java runtime (preferably 7) along with maven 3 to run this demo. Start DSE 3.1.X or a cassandra 1.2.X instance on your local machine. This demo just runs as a standalone process on the localhost.

This demo uses quite a lot of memory so it is worth setting the MAVEN_OPTS to run maven with more memory

    export MAVEN_OPTS=-Xmx512M

## Queries

The queries that we want to be able to run is 
	
1. Get all the tick data for a symbol in an exchange (in a time range)

	select * from historic_data where exchange ='AMEX' and symbol='ELG';

	select * from historic_data where exchange ='AMEX' and symbol='ELG' and date > '20014-01-01 14:45:00' and date < '2014-01-01 15:00:00';

3
## Data 

The data is generated from a tick generator which uses a csv file to create random values from AMEX, NYSE and NASDAQ.

## Throughput 

To increase the throughput, add nodes to the cluster. Cassandra will scale linearly with the amount of nodes in the cluster.

## Schema Setup
Note : This will drop the keyspace "datastax_tickdata_demo" and create a new one. All existing data will be lost. 

To specify contact points use the contactPoints command line parameter e.g. '-DcontactPoints=192.168.25.100,192.168.25.101'
The contact points can take mulitple points in the IP,IP,IP (no spaces).

To create the a single node cluster with replication factor of 1 for standard localhost setup, run the following

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaSetup"

To run the insert

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.tickdata.Main"
    
The default is to use 5 threads but this can be changed by using the noOfThreads property. 

An example of running this with 30 threads, 10,000,000 ticks and some custom contact points would be 

	mvn clean compile exec:java -Dexec.mainClass="com.datastax.tickdata.Main" -DcontactPoints=cassandra1 -DnoOfThreads=30 -DnoOfTicks=10000000
	
To remove the tables and the schema, run the following.

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaTeardown"
	

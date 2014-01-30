package com.datastax.tickdata;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.tickdata.model.TickData;

public class TickDataDao {

	private AtomicLong TOTAL_POINTS = new AtomicLong(0);
	private Session session;
	private static String keyspaceName = "datastax_tickdata_demo";
	private static String tableNameTick = keyspaceName + ".tick_data";

	private static final String INSERT_INTO_TICK = "Insert into " + tableNameTick + " (symbol,date,value) values (?,?,?);";

	private PreparedStatement insertStmtTick;

	public TickDataDao(String[] contactPoints) {

		Cluster cluster = Cluster.builder().addContactPoints(contactPoints).build();
		this.session = cluster.connect();

		this.insertStmtTick = session.prepare(INSERT_INTO_TICK);		
		this.insertStmtTick.setConsistencyLevel(ConsistencyLevel.ONE);
	}

	public void insertTickData(List<TickData> list) throws Exception{
		BoundStatement boundStmt = new BoundStatement(this.insertStmtTick);
		List<ResultSetFuture> results = new ArrayList<ResultSetFuture>();
		
		for (TickData tickData : list) {

			boundStmt.setString("symbol", tickData.getKey());
			boundStmt.setDate("date", new Date(System.currentTimeMillis()));
			boundStmt.setDouble("value", tickData.getValue());

			results.add(session.executeAsync(boundStmt));
			
			TOTAL_POINTS.incrementAndGet();
		}
		
		//Wait till we have everything back.
		boolean wait = true;
		while (wait) {
			// start with getting out, if any results are not done, wait is
			// true.
			wait = false;
			for (ResultSetFuture result : results) {
				if (!result.isDone()) {
					wait = true;
					break;
				}
			}
		}
		return;
	}

	public long getTotalPoints() {
		return TOTAL_POINTS.get();
	}
}

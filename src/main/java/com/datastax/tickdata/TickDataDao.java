package com.datastax.tickdata;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cern.colt.list.DoubleArrayList;
import cern.colt.list.LongArrayList;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy.Snapshot;
import com.datastax.driver.core.policies.LatencyAwarePolicy.Snapshot.Stats;
import com.datastax.driver.core.policies.Policies;
import com.datastax.tickdata.model.TickData;
import com.datastax.timeseries.utils.TimeSeries;

public class TickDataDao {
	
	private static Logger logger = LoggerFactory.getLogger(TickDataDao.class);
	
	private AtomicLong TOTAL_POINTS = new AtomicLong(0);
	private Session session;
	private static String keyspaceName = "datastax_tickdata_demo";
	private static String tableNameTick = keyspaceName + ".tick_data";

	private static final String INSERT_INTO_TICK = "Insert into " + tableNameTick + " (symbol,date,value) values (?, ?,?);";
	private static final String SELECT_FROM_TICK_RANGE = "Select symbol, date as date, value from " + tableNameTick + " where symbol = ? and date > ? and date < ?";
	private static final String SELECT_FROM_TICK = "Select symbol, date as date, value from " + tableNameTick + " where symbol = ?";

	private static final String SELECT_ALL = "Select * from " + tableNameTick;
	
	private PreparedStatement insertStmtTick;
	private PreparedStatement selectStmtTick;
	private PreparedStatement selectRangeStmtTick;
	
	private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
	
	private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.zzz"); 

	public TickDataDao(String[] contactPoints) {

		final LatencyAwarePolicy latencyPolicy = LatencyAwarePolicy.builder(Policies.defaultLoadBalancingPolicy()).build();
		
		final Cluster cluster = Cluster.builder()
				.addContactPoints(contactPoints)
				.withLoadBalancingPolicy(latencyPolicy)
				.build();
		
		executorService.scheduleAtFixedRate(new Runnable(){

			@Override
			public void run() {
				Snapshot scoresSnapshot = latencyPolicy.getScoresSnapshot();
				
				Map<Host, Stats> statsMap = scoresSnapshot.getAllStats();
				
				for (Host host : statsMap.keySet()){
					
					Stats stats = statsMap.get(host);					
					logger.info(host.getAddress() + " - Score:" + stats.getLatencyScore() + ", Count:" + stats.getMeasurementsCount() + ", Last updated:" + new Date(stats.lastUpdatedSince()));
				}
			}
		}, 5, 5, TimeUnit.SECONDS);
		
		this.session = cluster.connect();

		this.insertStmtTick = session.prepare(INSERT_INTO_TICK);		
		this.insertStmtTick.setConsistencyLevel(ConsistencyLevel.ONE);
		this.selectStmtTick = session.prepare(SELECT_FROM_TICK);		
		this.selectStmtTick.setConsistencyLevel(ConsistencyLevel.ONE);
		this.selectRangeStmtTick = session.prepare(SELECT_FROM_TICK_RANGE);		
		this.selectRangeStmtTick.setConsistencyLevel(ConsistencyLevel.ONE);
	}
	
	public TimeSeries getTickData(String symbol){
		
		BoundStatement boundStmt = new BoundStatement(this.selectStmtTick);
		boundStmt.setString(0, symbol);
		
		ResultSet resultSet = session.execute(boundStmt);		
		Iterator<Row> iterator = resultSet.iterator();
		
		DoubleArrayList values = new DoubleArrayList();
		LongArrayList dates = new LongArrayList();

		while (iterator.hasNext()) {
			Row row = iterator.next();

			dates.add(row.getDate("date").getTime());
			values.add(row.getDouble("value"));
		}

		dates.trimToSize();
		values.trimToSize();
		
		return new TimeSeries(symbol, dates.elements(), values.elements());
	}

	
	public TimeSeries getTickData(String symbol, long startTime, long endTime){
		
		BoundStatement boundStmt = new BoundStatement(this.selectRangeStmtTick);
		boundStmt.setString(0, symbol);
		boundStmt.setDate(1, new DateTime(startTime).toDate());
		boundStmt.setDate(2, new DateTime(endTime).toDate());
		
		ResultSet resultSet = session.execute(boundStmt);		
		Iterator<Row> iterator = resultSet.iterator();
		
		DoubleArrayList values = new DoubleArrayList();
		LongArrayList dates = new LongArrayList();

		while (iterator.hasNext()) {
			Row row = iterator.next();

			dates.add(row.getDate("date").getTime());
			values.add(row.getDouble("value"));
		}

		dates.trimToSize();
		values.trimToSize();
		
		return new TimeSeries(symbol, dates.elements(), values.elements());
	}
	
	public void insertTickData(TickData tickData) throws Exception{
		
		BoundStatement boundStmt = new BoundStatement(this.insertStmtTick);		
		DateTime dateTime = tickData.getTime() != null ? tickData.getTime() : DateTime.now();
		
		String month = fillNumber(dateTime.getMonthOfYear());
		String day = fillNumber(dateTime.getDayOfMonth());
		
		String symbolWithDate = tickData.getKey() + "-" + dateTime.getYear() + "-" + month + "-" + day;
		
		boundStmt.setString(0, symbolWithDate);
		boundStmt.setDate(1, new Timestamp(dateTime.getMillis()));
		boundStmt.setDouble(2, tickData.getValue());

		session.executeAsync(boundStmt);
			
		TOTAL_POINTS.incrementAndGet();				
	}

	public void insertTickData(List<TickData> list) throws Exception{
		BoundStatement boundStmt = new BoundStatement(this.insertStmtTick);
		List<ResultSetFuture> results = new ArrayList<ResultSetFuture>();
		
		for (TickData tickData : list) {
			
			DateTime dateTime = tickData.getTime() != null ? tickData.getTime() : DateTime.now();
			
			String month = fillNumber(dateTime.getMonthOfYear());
			String day = fillNumber(dateTime.getDayOfMonth());
			
			String symbolWithDate = tickData.getKey() + "-" + dateTime.getYear() + "-" + month + "-" + day;
			
			boundStmt.setString(0, symbolWithDate);
			boundStmt.setDate(1, new Timestamp(dateTime.getMillis()));
			boundStmt.setDouble(2, tickData.getValue());

			results.add(session.executeAsync(boundStmt));
			
			TOTAL_POINTS.incrementAndGet();			
		}
		
		for (ResultSetFuture future : results) {
			future.getUninterruptibly();
		}
		return;
	}
	
	public void selectAllHistoricData(int fetchSize){
		Statement stmt = new SimpleStatement(SELECT_ALL);
		stmt.setFetchSize(fetchSize);
		ResultSet rs = session.execute(stmt);
		
		Iterator<Row> iterator = rs.iterator();
		
		while (iterator.hasNext()){
			iterator.next().getDouble("value");
		}		
	}

	private String fillNumber(int num) {
		return num < 10 ? "0" + num : "" + num;
	}

	public long getTotalPoints() {
		return TOTAL_POINTS.get();
	}
}

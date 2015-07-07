package com.datastax.tickdata;

import java.text.NumberFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.tickdata.engine.TickGenerator;
import com.datastax.tickdata.model.TickData;

public class RunQueries {
	private static Logger logger = LoggerFactory.getLogger(RunQueries.class);

	public RunQueries() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		TickDataDao dao = new TickDataDao(contactPointsStr.split(","));

		int fetchSize = 50000;
		
		Timer timer = new Timer();
		dao.selectAllHistoricData(fetchSize);
		timer.end();
		
		logger.info("Select * took : " + timer.getTimeTakenSeconds() + "secs for fetchsize : " + fetchSize);
				
		System.exit(0);
	}
		
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new RunQueries();
	}
}

package com.datastax.tickdata;

import java.text.NumberFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.tickdata.engine.TickGenerator;
import com.datastax.tickdata.engine.TickGenerator.TickValue;
import com.datastax.tickdata.model.TickData;

public class MainRatePerSec {
	private static Logger logger = LoggerFactory.getLogger(MainRatePerSec.class);

	private String ONE_MILLION = "1000000";
	private String TEN_MILLION = "10000000";
	private String FIFTY_MILLION = "50000000";
	private String ONE_HUNDRED_MILLION = "100000000";
	private String ONE_BILLION = "1000000000";

	public MainRatePerSec() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		String ratePerSecStr = PropertyHelper.getProperty("ratePerSec", "1000");
		String noOfTicksStr = PropertyHelper.getProperty("noOfTicks", ONE_MILLION);
		
		TickDataDao dao = new TickDataDao(contactPointsStr.split(","));
		
		long noOfTicks = Long.parseLong(noOfTicksStr);
		int ratePerSec = Integer.parseInt(ratePerSecStr);
		Timer timer = new Timer();
		timer.start();
			
		logger.info("Processing " + NumberFormat.getInstance().format(noOfTicks) + " ticks");
		
		//Load the symbols
		DataLoader dataLoader = new DataLoader ();
		List<String> exchangeSymbols = dataLoader.getExchangeData();
		
		//Start the tick generator
		TickGenerator tickGenerator = new TickGenerator(exchangeSymbols);
		startLogging(tickGenerator);
		for (int i=0; i < noOfTicks; i++){
			DateTime dateTime = DateTime.now();
			
			TickValue tickValue = tickGenerator.getTickValueRandom();
			try {
				dao.insertTickData(new TickData(tickValue.tickSymbol, tickValue.value, dateTime));
			} catch (Exception e) {
				e.printStackTrace();
			}
			sleep(1000/ratePerSec);
		}
		
		timer.end();
		logger.info("Data Loading took " + timer.getTimeTakenSeconds() + " secs. Total Points " + dao.getTotalPoints() + " (" + (dao.getTotalPoints()/timer.getTimeTakenSeconds()) + " a sec)");
		
		System.exit(0);
	}
	
	private void startLogging(final TickGenerator tickGenerator) {
		ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(5);

		scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
			@Override
			public void run() {
				logger.info(new Date().toString() + "-Generated " + tickGenerator.getTicksGenerated() + " ticks");
			}
		}, 1, 5, TimeUnit.SECONDS);		
	}

	class TickDataWriter implements Runnable {

		private TickDataDao dao;
		private BlockingQueue<List<TickData>> queue;

		public TickDataWriter(TickDataDao dao, BlockingQueue<List<TickData>> queue) {
			this.dao = dao;
			this.queue = queue;
		}

		@Override
		public void run() {
			List<TickData> list;
			while(true){				
				list = queue.poll(); 
				
				if (list!=null){
					try {
						this.dao.insertTickData(list);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}				
			}				
		}
	}
	
	private void sleep(int millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new MainRatePerSec();
	}
}

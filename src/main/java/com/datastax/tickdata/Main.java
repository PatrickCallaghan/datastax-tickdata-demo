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
import java.util.concurrent.atomic.AtomicLong;

import org.mortbay.log.Log;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.tickdata.engine.TickGenerator;
import com.datastax.tickdata.model.TickData;

public class Main {

	private String ONE_MILLION = "1000000";
	private String TEN_MILLION = "10000000";
	private String FIFTY_MILLION = "50000000";
	private String ONE_HUNDRED_MILLION = "100000000";
	private String ONE_BILLION = "1000000000";

	public Main() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		String noOfThreadsStr = PropertyHelper.getProperty("noOfThreads", "10");
		String noOfTicksStr = PropertyHelper.getProperty("noOfTicks", ONE_MILLION);
		
		TickDataDao dao = new TickDataDao(contactPointsStr.split(","));
		
		int noOfThreads = Integer.parseInt(noOfThreadsStr);
		long noOfTicks = Long.parseLong(noOfTicksStr);		
		//Create shared queue 
		BlockingQueue<List<TickData>> queueTickData = new ArrayBlockingQueue<List<TickData>>(10000);
		
		//Executor for Threads
		ExecutorService executor = Executors.newFixedThreadPool(noOfThreads);
		Timer timer = new Timer();
		timer.start();
		
		Log.info("Processing " + NumberFormat.getInstance().format(noOfTicks) + " ticks with " + noOfThreads + " threads");
		
		for (int i = 0; i < noOfThreads; i++) {
			executor.execute(new TickDataWriter(dao, queueTickData));
		}
		
		//Load the symbols
		DataLoader dataLoader = new DataLoader (queueTickData);
		List<String> exchangeSymbols = dataLoader.getExchangeData();
		
		//Start the tick generator
		TickGenerator tickGenerator = new TickGenerator(exchangeSymbols);
		
		startLogging(tickGenerator, queueTickData);			
		tickGenerator.generatorTicks(queueTickData, noOfTicks);
		
		while(!queueTickData.isEmpty() ){
			sleep(1);
		}		
		
		timer.end();
		Log.info("Data Loading took " + timer.getTimeTakenSeconds() + " secs. Total Points " + dao.getTotalPoints() + " (" + (dao.getTotalPoints()/timer.getTimeTakenSeconds()) + " a sec)");
		
		System.exit(0);
	}
	
	private void startLogging(final TickGenerator tickGenerator, final BlockingQueue<List<TickData>> queueTickData) {
		ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(5);

		scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
			@Override
			public void run() {
				Log.info(new Date().toString() + "-Generated " + tickGenerator.getTicksGenerated() + " ticks");
				Log.info("Messages left to send " + (queueTickData.size()));
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
	
	private void sleep(int seconds) {
		try {
			Thread.sleep(seconds * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new Main();
	}
}
